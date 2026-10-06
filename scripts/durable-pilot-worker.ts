import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { RelayServer, type RelayPubSubAdapter } from '@cgp/relay/src/server';
import { WebSocketPubSubHub, RedundantWebSocketRelayPubSubAdapter } from '@cgp/relay/src/pubsub_ws';
import { getPublicKey } from '@cgp/core';
async function main() {
    const directory = path.resolve(process.argv[2] || '');
    if (!directory.includes('hollow-roadmap-staging-20260928-durable-pilot'))
        throw Error('Explicit pilot directory required');
    const config = JSON.parse(fs.readFileSync(path.join(directory, 'config.json'), 'utf8'));
    if (config.relayPort !== 21480 || config.hubPort !== 21481)
        throw Error('Pilot port mismatch');
    const hub = new WebSocketPubSubHub(config.hubPort, { host: '127.0.0.1', authToken: config.pubsubToken, retainDir: path.join(directory, 'hub-retained') });
    const subscriptions = new Set<any>();
    let adapter: RedundantWebSocketRelayPubSubAdapter | undefined;
    function connect() { adapter = new RedundantWebSocketRelayPubSubAdapter(config.pubsubUrls, { authToken: config.pubsubToken }); for (const sub of subscriptions)
        sub.remove = adapter.subscribe(sub.topic, sub.handler, sub.options); }
    connect();
    let isolated = false;
    const bus: RelayPubSubAdapter = { publish(topic, event) { return adapter?.publish(topic, event); }, subscribe(topic, handler, options) { const sub = { topic, handler, options, remove: adapter?.subscribe(topic, handler, options) }; subscriptions.add(sub); return () => { sub.remove?.(); subscriptions.delete(sub); }; }, isReady() { return adapter?.isReady() ?? false; } };
    const relay = new RelayServer(config.relayPort, path.join(directory, 'leveldb'), [], { listenHost: '127.0.0.1', enableDefaultPlugins: false, sequencerConsensus: { epoch: config.quorum.epoch, members: config.quorum.members, requiredVotes: config.quorum.requiredVotes }, instanceId: config.label, relayPrivateKeyHex: config.privateKey, pubSubAdapter: bus, writeQuorum: config.quorum });
    const tunnel = spawn(config.cloudflared, ['tunnel', '--config', path.join(directory, 'tunnel.yml'), '--no-autoupdate', '--protocol', 'http2', 'run', config.tunnelId], { windowsHide: true, stdio: ['ignore', 'ignore', 'ignore'] });
    const metadata = { machine: os.hostname(), pid: process.pid, tunnelPid: tunnel.pid, relayPort: config.relayPort, hubPort: config.hubPort, publicKey: getPublicKey(Buffer.from(config.privateKey, 'hex')), hostname: config.hostname, quorum: config.quorum, startedAt: new Date().toISOString() };
    fs.writeFileSync(path.join(directory, 'ready.json'), JSON.stringify(metadata, null, 2));
    let closing = false;
    async function close(code = 0) { if (closing)
        return; closing = true; clearInterval(watch); tunnel.kill(); await relay.close(); await adapter?.close(); await hub.close(); fs.writeFileSync(path.join(directory, 'stopped.json'), JSON.stringify({ ...metadata, stoppedAt: new Date().toISOString(), exitCode: code })); process.exit(code); }
    const watch = setInterval(() => {
        if (fs.existsSync(path.join(directory, 'stop'))) {
            void close();
            return;
        }
        const requested = fs.existsSync(path.join(directory, 'isolate'));
        if (requested !== isolated) {
            isolated = requested;
            const old = adapter;
            adapter = undefined;
            void old?.close();
            if (!isolated)
                connect();
        }
        fs.writeFileSync(path.join(directory, 'transport-state.json'), JSON.stringify({ isolated, ready: adapter?.isReady() ?? false, hubs: (adapter as any)?.adapters?.map((a: any) => a.isReady()) ?? [], observedAt: new Date().toISOString() }));
    }, 500);
    tunnel.once('exit', () => { if (!closing)
        void close(1); });
    tunnel.once('error', () => void close(1));
    process.once('SIGINT', () => void close());
    process.once('SIGTERM', () => void close());
}
void main().catch(() => { console.error('Durable pilot worker failed'); process.exit(1); });
