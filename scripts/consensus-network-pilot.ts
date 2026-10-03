// Synthetic, TTL-limited public transport validation. No persistent deployment.
import { allowFixtureHostname, fixtureDnsObservations } from './fixture-dns.js';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, type ChildProcess } from 'node:child_process';
import { randomUUID, createHash } from 'node:crypto';
import { WebSocket } from 'ws';
import { ConsensusV2Client } from '@cgp/client';
import { generatePrivateKey, getPublicKey, hashObject, sign, verify, consensusUnsigned, computeEventId,
  consensusPolicyHash, consensusTransitionPayload, verifyConsensusHistory,
  type ConsensusPolicy, type ConsensusTransition, type GuildEvent, type EventBody } from '@cgp/core';
import { echoPowerShell, workerPowerShell, copyToEcho } from '../../hollow-tauri/scripts/ops/remote-ssh.mjs';

type Host = 'local' | 'echo' | 'cortop3';
type Fixture = { host: Host; label: string; directory: string; index: number; ready?: any };
const id = `consensus-network-${Date.now()}`;
const output = path.resolve(`output/community-continuity/${id}`);
fs.mkdirSync(output, { recursive: true });
const bundle = path.resolve('output/community-continuity/remote-package/consensus-worker.cjs');
const deps = path.resolve('output/community-continuity/remote-package/dependencies.zip');
const keys = Array.from({ length: 4 }, () => generatePrivateKey()), members = keys.map(getPublicKey);
const ownerKey = generatePrivateKey(), owner = getPublicKey(ownerKey), guildId = hashObject({ id, owner });
const anchor: ConsensusPolicy = { epoch: `${id}-abc`, members: members.slice(0, 3).sort(), requiredVotes: 2, administrators: [owner], requiredAdministrators: 1 };
const fixtures: Fixture[] = [
  { host: 'local', label: 'A', index: 0, directory: path.join(output, `hollow-roadmap-staging-${id}-a`) },
  { host: 'echo', label: 'B', index: 1, directory: `C:/Users/ECHO/hollow-roadmap-staging-${id}-b` },
  { host: 'cortop3', label: 'C', index: 2, directory: `C:/Users/PC/hollow-roadmap-staging-${id}-c` },
  { host: 'local', label: 'D', index: 3, directory: path.join(output, `hollow-roadmap-staging-${id}-d`) },
];
const cloudflared = { local: 'C:/Users/cortanium/Downloads/asda/tools/cloudflared-2026.6.0-windows-amd64.exe',
  echo: 'C:/Program Files (x86)/cloudflared/cloudflared.exe', cortop3: 'C:/Users/PC/Downloads/cloudflared/cloudflared.exe' };
const quote = (s: string) => `'${s.replaceAll("'", "''")}'`;
const encode = (s: string) => Buffer.from(s, 'utf16le').toString('base64');
const processes: ChildProcess[] = [], clients: ConsensusV2Client[] = [], sockets = new Set<WebSocket>();
const started = new Set<Fixture>(), instances: any[] = [], checks: string[] = [], failures: any[] = [];
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
function save(name: string, data: unknown) { fs.writeFileSync(path.join(output, name), JSON.stringify(data, null, 2)); }
async function remote(host: Host, script: string) { return host === 'echo' ? echoPowerShell(script) : workerPowerShell('cortop3', script); }
async function read(f: Fixture, name: string) {
  const file = `${f.directory}/${name}`;
  let raw: string;
  if (f.host === 'local') { if (!fs.existsSync(file)) return undefined; raw = fs.readFileSync(file, 'utf8'); }
  else raw = (await remote(f.host, `if(Test-Path -LiteralPath ${quote(file)}){Get-Content -LiteralPath ${quote(file)} -Raw}`)).stdout;
  try { return raw.trim() ? JSON.parse(raw) : undefined; } catch { return undefined; }
}
async function write(f: Fixture, name: string, value: string) {
  if (f.host === 'local') fs.writeFileSync(path.join(f.directory, name), value);
  else await remote(f.host, `[IO.File]::WriteAllText(${quote(f.directory+'/'+name)},${quote(value)})`);
}
async function removeControl(f: Fixture, names: string[]) {
  for (const name of names) {
    if (!['stop','stopped.json','ready.json','isolate','state.json'].includes(name)) throw Error('Not an owned control file');
    if (f.host === 'local') fs.rmSync(path.join(f.directory, name), { force: true });
    else await remote(f.host, `if(Test-Path -LiteralPath ${quote(f.directory+'/'+name)}){Remove-Item -LiteralPath ${quote(f.directory+'/'+name)} -ErrorAction Stop}`);
  }
}
async function waitFor(label: string, operation: () => Promise<boolean>, seconds = 120) {
  const end = Date.now() + seconds * 1000;
  while (Date.now() < end) { if (await operation()) return; await pause(700); }
  throw Error(`Timed out: ${label}`);
}
async function prepare() {
  const b = fixtures[1], c = fixtures[2];
  for (const f of fixtures) {
    if (f.host === 'local') fs.mkdirSync(f.directory, { recursive: true });
    else await remote(f.host, `New-Item -ItemType Directory -Path ${quote(f.directory)} -ErrorAction Stop | Out-Null`);
  }
  await copyToEcho(bundle, `${b.directory}/consensus-worker.cjs`);
  await copyToEcho(deps, `${b.directory}/dependencies.zip`);
  for (const filename of ['consensus-worker.cjs','dependencies.zip']) {
    await echoPowerShell(`& scp -F C:/Users/ECHO/.workers/config -o BatchMode=yes -o StrictHostKeyChecking=yes ${quote(b.directory+'/'+filename)} ${quote('cortop3:'+c.directory+'/'+filename)}; exit $LASTEXITCODE`, { timeout: 180000 });
  }
  for (const f of [b,c]) await remote(f.host, `Expand-Archive -LiteralPath ${quote(f.directory+'/dependencies.zip')} -DestinationPath ${quote(f.directory)} -ErrorAction Stop`);
  save('bundle.json', { sha256: createHash('sha256').update(fs.readFileSync(bundle)).digest('hex'), dependenciesSha256: createHash('sha256').update(fs.readFileSync(deps)).digest('hex'), ttlMinutes: 30 });
}
async function start(f: Fixture) {
  await removeControl(f, ['stop','stopped.json','ready.json','state.json']);
  await write(f, 'config.json', JSON.stringify({ guildId, anchor, label: f.label, privateKey: Buffer.from(keys[f.index]).toString('hex'), cloudflared: cloudflared[f.host] }));
  let process: ChildProcess;
  if (f.host === 'local') process = spawn(globalThis.process.execPath, [bundle, f.directory], { windowsHide: true, stdio: ['ignore','pipe','pipe'] });
  else {
    const node = f.host === 'echo' ? 'C:/nvm4w/nodejs/node.exe' : 'C:/Users/PC/hollow-roadmap-staging-20260926-v3/runtime/node.exe';
    const inner = `Set-Location -LiteralPath ${quote(f.directory)}; & ${quote(node)} ${quote(f.directory+'/consensus-worker.cjs')} ${quote(f.directory)}`;
    const script = f.host === 'echo' ? inner : `& ssh -F C:/Users/ECHO/.workers/config -o BatchMode=yes -o StrictHostKeyChecking=yes cortop3 'powershell -NoProfile -NonInteractive -EncodedCommand ${encode(inner)}'; exit $LASTEXITCODE`;
    process = spawn('ssh', ['-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','ConnectTimeout=8','echo.hardesty.ai',`powershell -NoProfile -NonInteractive -EncodedCommand ${encode(script)}`], { windowsHide: true, stdio: ['ignore','pipe','pipe'] });
  }
  const logfile = fs.createWriteStream(path.join(output, `${f.label}-${Date.now()}-worker.log`));
  process.stdout?.pipe(logfile); process.stderr?.pipe(logfile); process.once('exit', () => logfile.end());
  processes.push(process); started.add(f);
  await waitFor(`${f.label} tunnel`, async () => !!(f.ready = await read(f, 'ready.json')));
  assert.equal(f.ready.host, '127.0.0.1'); assert.equal(f.ready.publicKey, members[f.index]);
  allowFixtureHostname(f.ready.wssUrl); instances.push({ routeHost: f.host, directory: f.directory, ...f.ready }); save('instances.json', instances);
  console.log(JSON.stringify({ stage: 'ready', label: f.label, machine: f.ready.machine, pid: f.ready.pid, url: f.ready.wssUrl }));
}
async function distributePeers() {
  const peers = Object.fromEntries(fixtures.map(f => [members[f.index], f.ready.wssUrl]));
  await Promise.all(fixtures.map(f => write(f, 'peers.json', JSON.stringify(peers))));
  await waitFor('peer maps', async () => (await Promise.all(fixtures.map(f => read(f, 'state.json')))).every(state => state?.peerCount === 4), 30);
}
function client(f: Fixture, key = ownerKey) {
  const value = new ConsensusV2Client({ relayUrl: f.ready.wssUrl, guildId, anchorPolicy: anchor, timeoutMs: 60000,
    readSigner: async payload => ({ author: getPublicKey(key), signature: await sign(key, hashObject(payload)) }) });
  clients.push(value); return value;
}
async function rpc(f: Fixture, method: string, payload?: unknown, signer = 0): Promise<any> {
  const unsigned = { protocol: 'cgp/consensus-rpc/2', requestId: randomUUID(), guildId, method,
    ...(payload === undefined ? {} : { payload }), createdAt: Date.now(), relayPublicKey: members[signer] };
  const request = { ...unsigned, signature: await sign(keys[signer], hashObject(unsigned)) };
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(f.ready.wssUrl); sockets.add(socket);
    let settled = false;
    const finish = (error?: Error, result?: unknown) => { if(settled)return; settled=true; clearTimeout(timer); sockets.delete(socket); socket.terminate(); error?reject(error):resolve(result); };
    const timer = setTimeout(() => finish(Error('Signed fixture RPC timeout')), 14000);
    socket.on('error', error => finish(error)); socket.on('close', () => finish(Error('Fixture RPC closed')));
    socket.on('open', () => socket.send(JSON.stringify(['CONSENSUS_RPC', request])));
    socket.on('message', data => { try {
      const [kind, response] = JSON.parse(data.toString());
      if (kind === 'CONSENSUS_ERROR' && response.requestId === request.requestId) return finish(Error(response.message));
      if (kind !== 'CONSENSUS_RPC_RESULT' || response.requestId !== request.requestId) return;
      assert.equal(response.guildId, guildId); assert.equal(response.relayPublicKey, members[f.index]);
      assert.equal(verify(members[f.index], hashObject(consensusUnsigned(response)), response.signature), true);
      finish(response.error ? Error(response.error) : undefined, response.result);
    } catch(error) { finish(error as Error); } });
  });
}
async function reachable(f: Fixture) {
  await waitFor(`${f.label} authenticated WSS`, async () => { try { await rpc(f, 'status'); return true; } catch(error) { failures.push({ stage:'reachability', label:f.label, error:(error as Error).message }); return false; } });
}
async function makeEvent(body: EventBody, seq: number, previous: string | null): Promise<GuildEvent> {
  const createdAt = Date.now(), author = owner, signature = await sign(ownerKey, hashObject({ body, author, createdAt }));
  const event = { body, author, createdAt, signature, seq, prevHash: previous } as GuildEvent;
  return { ...event, id: computeEventId(event) };
}
async function stop(f: Fixture) {
  await write(f, 'stop', 'controller-owned cleanup');
  await waitFor(`${f.label} stopped`, async () => !!await read(f, 'stopped.json'), 35);
}
async function main() {
  await prepare();
  for(const f of fixtures) await start(f);
  await distributePeers(); await Promise.all(fixtures.map(reachable));
  checks.push('four authenticated public WSS voter identities on three physical PCs; TLS verification enabled');
  let a = client(fixtures[0]);
  const genesis = await makeEvent({ type:'GUILD_CREATE',guildId,name:'Synthetic private network pilot',access:'private' },0,null);
  assert.equal((await a.submitEvent(genesis)).requestedCommitted,true);
  await assert.rejects(client(fixtures[1],generatePrivateKey()).fetchHistory(), /access denied/i);
  checks.push('private genesis committed and signed outsider history read rejected');
  const selected = await makeEvent({type:'CHANNEL_CREATE',guildId,channelId:'selected',name:'selected',kind:'text'},1,genesis.id);
  const other = await makeEvent({type:'CHANNEL_CREATE',guildId,channelId:'other',name:'other',kind:'text'},1,genesis.id);
  const scope = await rpc(fixtures[0],'status');
  const prepUnsigned = {protocol:'cgp/consensus-prepare/2',guildId,index:scope.index,parentHash:scope.parentHash,policyHash:scope.policyHash,ballot:{counter:100,proposer:members[0]}};
  const prepareRequest = {...prepUnsigned,signature:await sign(keys[0],hashObject(prepUnsigned))};
  const promises = await Promise.all(fixtures.slice(0,3).map(f=>rpc(f,'prepare',prepareRequest)));
  const acceptUnsigned = {...prepUnsigned,protocol:'cgp/consensus-accept/2',value:{kind:'event',event:selected},promises};
  const accept = {...acceptUnsigned,signature:await sign(keys[0],hashObject(acceptUnsigned))};
  await Promise.all(fixtures.slice(0,2).map(f=>rpc(f,'accept',accept)));
  for(const f of fixtures.slice(0,2)) { await stop(f); await start(f); }
  a.close(); await distributePeers(); await Promise.all(fixtures.slice(0,2).map(reachable));
  const c = client(fixtures[2]), recovered = await c.submitEvent(other);
  assert.equal(recovered.requestedCommitted,false); assert.equal(recovered.verified.events.at(-1)?.id,selected.id);
  checks.push('A/B majority accepted without commit, both restarted same LevelDB/key, C recovered exact earlier value over competing request');
  const nextPolicy = {...anchor,epoch:`${id}-bcd`,members:members.slice(1).sort()};
  const transition:ConsensusTransition={kind:'transition',guildId,fromPolicyHash:consensusPolicyHash(anchor),parentHash:recovered.verified.scope.parentHash,nextPolicy,nonce:id,signatures:[]};
  transition.signatures.push({publicKey:owner,signature:await sign(ownerKey,hashObject(consensusTransitionPayload(transition)))});
  const moved = await c.submitTransition(transition); assert.equal(moved.requestedCommitted,true); assert.equal(moved.verified.policy.epoch,nextPolicy.epoch); assert.equal(moved.verified.pendingTransition,undefined);
  // Explicit retirement evidence: old A cannot authenticate a prepare into BCD.
  const afterScope=moved.verified.scope;
  const retiredUnsigned={protocol:'cgp/consensus-prepare/2',...afterScope,ballot:{counter:101,proposer:members[0]}};
  await assert.rejects(rpc(fixtures[1],'prepare',{...retiredUnsigned,signature:await sign(keys[0],hashObject(retiredUnsigned))}),/authorized|Invalid|voter/i);
  const d=client(fixtures[3]), after=await makeEvent({type:'CHANNEL_CREATE',guildId,channelId:'after',name:'after',kind:'text'},2,selected.id);
  assert.equal((await d.submitEvent(after)).requestedCommitted,true);
  checks.push('ABC→BCD administrator/old-majority/new-majority transition activates, old A refused, new D writes with BCD spanning three PCs');
  await write(fixtures[1],'isolate','bounded synthetic RPC partition'); await write(fixtures[2],'isolate','bounded synthetic RPC partition');
  await waitFor('B/C isolated',async()=> (await Promise.all(fixtures.slice(1,3).map(f=>read(f,'state.json')))).every(s=>s?.isolated),30);
  const minority=await makeEvent({type:'CHANNEL_CREATE',guildId,channelId:'healed',name:'healed',kind:'text'},3,after.id);
  await assert.rejects(d.submitEvent(minority),/quorum unavailable/i);
  assert.equal((await d.fetchHistory()).verified.events.at(-1)?.id,after.id);
  checks.push('D alone cannot commit under unchanged BCD 2-of-3 policy; certified prefix unchanged');
  for(const f of fixtures.slice(1,3)) await removeControl(f,['isolate']);
  await waitFor('B/C healed',async()=> (await Promise.all(fixtures.slice(1,3).map(f=>read(f,'state.json')))).every(s=>s?.isolated===false),30);
  const healed=await d.submitEvent(minority); assert.equal(healed.requestedCommitted,true);
  const history=healed.history; verifyConsensusHistory(history,guildId,anchor); save('certified-history.json',history);
  for(const f of fixtures.slice(1)) { const result=await client(f).fetchHistory(); assert.equal(result.verified.events.at(-1)?.id,minority.id); }
  checks.push('healed B/C/D converge on certified history and exact refused request subsequently commits');
}
async function cleanup() {
  clients.forEach(c=>c.close()); for(const socket of sockets)socket.terminate();
  const receipts:any[]=[];
  for(const f of [...started].reverse()) { try { await stop(f); receipts.push({label:f.label,stopped:await read(f,'stopped.json'),dns:await read(f,'dns-observations.json')}); } catch(error) { receipts.push({label:f.label,error:(error as Error).message}); } }
  for(const process of processes)process.kill();
  save('cleanup.json',receipts); return receipts.every(receipt=>!receipt.error);
}
async function run() {
let failure:string|undefined;
try { await main(); } catch(error) { failure=(error as Error).message; save('failure.json',{message:failure,stack:(error as Error).stack}); }
finally { if(!await cleanup())failure=`${failure??''} cleanup unconfirmed`.trim(); }
save('dns.json',fixtureDnsObservations()); save('transient-failures.json',failures);
const report={id,at:new Date().toISOString(),ok:!failure,failure,checks,anchor,instances,
  scope:['temporary quick tunnels and foreground SSH workers only','30-minute worker TTL','three PCs under one owner; D is a new key on A host, not a new independent operator','signed RPC test controls and Node client; not browser/physical-media acceptance','fixture-only exact-host DNS fallback may be recorded; TLS SNI and verification unchanged']};
save('report.json',report); console.log(JSON.stringify({output,...report},null,2)); if(failure)process.exitCode=1;
}
void run();
