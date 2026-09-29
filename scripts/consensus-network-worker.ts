// Bounded synthetic network fixture: no installation, service or startup task.
import { allowFixtureHostname, fixtureDnsObservations } from './fixture-dns.js';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { RelayServer } from '@cgp/relay/src/server';
import { getPublicKey } from '@cgp/core';

async function main() {
  const directory = path.resolve(process.argv[2] ?? '');
  if (!path.basename(directory).startsWith('hollow-roadmap-staging-consensus-network-') || !fs.existsSync(directory)) throw Error('Owned network fixture directory required');
  const config = JSON.parse(fs.readFileSync(path.join(directory, 'config.json'), 'utf8'));
  const peers: Record<string, string> = {};
  const relay = new RelayServer(0, path.join(directory, 'leveldb'), [], {
    listenHost: '127.0.0.1', enableDefaultPlugins: false, writeQuorum: false, sequencerConsensus: false,
    relayPrivateKeyHex: config.privateKey,
    consensusV2: { guilds: { [config.guildId]: config.anchor }, peers, timeoutMs: 8000 },
  });
  const service = (relay as any).consensusV2;
  const rpc = service.rpc.bind(service), handleRpc = service.handleRpc.bind(service);
  let isolated = false;
  service.rpc = (...args: any[]) => isolated ? Promise.reject(Error('Synthetic fixture partition')) : rpc(...args);
  service.handleRpc = (...args: any[]) => isolated ? Promise.reject(Error('Synthetic fixture partition')) : handleRpc(...args);
  while (!relay.getPort()) await new Promise(resolve => setTimeout(resolve, 10));
  const metadata = { machine: os.hostname(), pid: process.pid, port: relay.getPort(), host: '127.0.0.1',
    publicKey: getPublicKey(Buffer.from(config.privateKey, 'hex')), label: config.label };
  const tunnel = spawn(config.cloudflared, ['tunnel', '--no-autoupdate', '--protocol', 'http2', '--url',
    `http://127.0.0.1:${metadata.port}`, '--metrics', '127.0.0.1:0'], { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  let logs = '', closing = false;
  const inspect = (chunk: Buffer) => {
    logs = (logs + chunk.toString()).slice(-24000);
    const url = logs.match(/https:\/\/[a-z0-9]+(?:-[a-z0-9]+){2,}\.trycloudflare\.com/);
    if (url) fs.writeFileSync(path.join(directory, 'ready.json'), JSON.stringify({ ...metadata, tunnelPid: tunnel.pid, wssUrl: url[0].replace('https:', 'wss:') }));
  };
  tunnel.stdout?.on('data', inspect); tunnel.stderr?.on('data', inspect);
  async function close() {
    if (closing) return; closing = true; clearInterval(watch); clearTimeout(deadline);
    tunnel.kill(); await relay.close();
    fs.writeFileSync(path.join(directory, 'stopped.json'), JSON.stringify({ ...metadata, tunnelPid: tunnel.pid, stoppedAt: new Date().toISOString() }));
    fs.writeFileSync(path.join(directory, 'dns-observations.json'), JSON.stringify(fixtureDnsObservations()));
    process.exit(0);
  }
  const watch = setInterval(() => {
    if (fs.existsSync(path.join(directory, 'stop'))) { void close(); return; }
    isolated = fs.existsSync(path.join(directory, 'isolate'));
    try {
      const incoming = JSON.parse(fs.readFileSync(path.join(directory, 'peers.json'), 'utf8'));
      for (const [key, url] of Object.entries(incoming)) { allowFixtureHostname(url as string); peers[key] = url as string; }
    } catch { /* The SSH controller may be replacing this fixture-owned file. */ }
    fs.writeFileSync(path.join(directory, 'state.json'), JSON.stringify({ isolated, peerCount: Object.keys(peers).length }));
  }, 200);
  const deadline = setTimeout(() => void close(), 30 * 60 * 1000);
  tunnel.once('error', () => void close()); tunnel.once('exit', () => void close());
  process.once('SIGINT', () => void close()); process.once('SIGTERM', () => void close());
}
void main().catch(() => { console.error('Bounded consensus network fixture failed'); process.exit(1); });
