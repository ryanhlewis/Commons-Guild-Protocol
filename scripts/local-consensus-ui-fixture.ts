// Synthetic loopback-only fixture for actual browser/native UI qualification.
import fs from 'node:fs';
import path from 'node:path';
import { RelayServer } from '@cgp/relay/src/server';
import { ConsensusV2Client } from '@cgp/client';
import { generatePrivateKey, getPublicKey, hashObject, sign, computeEventId, type GuildEvent, type ConsensusPolicy } from '@cgp/core';

async function main() {
  const directory = path.resolve(process.argv[2] ?? '');
  if (!path.basename(directory).startsWith('consensus-ui-')) throw Error('Owned consensus-ui directory required');
  fs.mkdirSync(directory, { recursive: true });
  const keys = Array.from({ length: 3 }, () => generatePrivateKey());
  const ownerKey = generatePrivateKey(), owner = getPublicKey(ownerKey);
  const guildId = hashObject({ directory, owner }), channelId = hashObject({ guildId, channel: 'native-validation' });
  const anchor: ConsensusPolicy = { epoch: 'ui-fixture', members: keys.map(getPublicKey).sort(), requiredVotes: 2, administrators: [owner], requiredAdministrators: 1 };
  const peers: Record<string, string> = {}, servers: RelayServer[] = [];
  let client: ConsensusV2Client | undefined;
  let isolated: number[] = [], commandBusy = false, lastCommand = '';
  let closing = false;
  const timers: ReturnType<typeof setInterval>[] = [];
  let deadline: ReturnType<typeof setTimeout> | undefined;
  const close = async (exitCode = 0) => {
    if (closing) return; closing = true; client?.close();
    timers.forEach(clearInterval); clearTimeout(deadline);
    const ports = servers.map(server => server.getPort());
    const results = await Promise.allSettled(servers.map(server => server.close()));
    const cleanupErrors = results.filter(result => result.status === 'rejected').map(result => String((result as PromiseRejectedResult).reason));
    const status = cleanupErrors.length ? 1 : exitCode;
    fs.writeFileSync(path.join(directory, 'stopped.json'), JSON.stringify({ pid: process.pid, ports, exitCode: status, cleanupErrors, stoppedAt: new Date().toISOString() }));
    process.exit(status);
  };
  process.once('SIGINT', () => void close()); process.once('SIGTERM', () => void close());
  timers.push(setInterval(() => { if (fs.existsSync(path.join(directory, 'stop'))) void close(); }, 250));
  timers.push(setInterval(() => {
    try {
      const incoming = JSON.parse(fs.readFileSync(path.join(directory, 'partition.json'), 'utf8'));
      if (Array.isArray(incoming) && incoming.every(index => Number.isInteger(index) && index >= 0 && index < 3)) isolated = [...new Set(incoming)];
    } catch { if (!fs.existsSync(path.join(directory, 'partition.json'))) isolated = []; }
    if (commandBusy || !client) return;
    let command: { id: string; publicKey: string };
    try { command = JSON.parse(fs.readFileSync(path.join(directory, 'add-member.json'), 'utf8')); } catch { return; }
    if (!command.id || command.id === lastCommand) return;
    lastCommand = command.id; commandBusy = true;
    void (async () => {
      try {
        if (!/^(02|03)[a-f0-9]{64}$/.test(command.publicKey)) throw Error('Invalid synthetic member key');
        const history = await client!.fetchHistory(), previous = history.verified.events.at(-1);
        const body = { type: 'ROLE_ASSIGN', guildId, userId: command.publicKey, roleId: 'member' };
        const unsigned = { body, author: owner, createdAt: Date.now() };
        const event = { ...unsigned, seq: history.verified.events.length, prevHash: previous?.id ?? null, signature: await sign(ownerKey, hashObject(unsigned)) } as GuildEvent;
        event.id = computeEventId(event);
        const result = await client!.submitEvent(event);
        fs.writeFileSync(path.join(directory, 'member-result.json'), JSON.stringify({ id: command.id, ok: result.requestedCommitted }));
      } catch (error) { fs.writeFileSync(path.join(directory, 'member-result.json'), JSON.stringify({ id: command.id, ok: false, error: error instanceof Error ? error.message : 'Fixture command failed' })); }
      finally { commandBusy = false; }
    })();
  }, 250));
  deadline = setTimeout(() => void close(), 30 * 60 * 1000);
  try {
    for (const key of keys) {
      const server = new RelayServer(0, path.join(directory, `node-${servers.length}`), [], {
        listenHost: '127.0.0.1', enableDefaultPlugins: false, writeQuorum: false, sequencerConsensus: false,
        relayPrivateKeyHex: Buffer.from(key).toString('hex'), consensusV2: { guilds: { [guildId]: anchor }, peers },
      });
      servers.push(server);
      const index = servers.length - 1, service = (server as any).consensusV2;
      const rpc = service.rpc.bind(service), handleRpc = service.handleRpc.bind(service);
      service.rpc = (...args: any[]) => isolated.includes(index) ? Promise.reject(Error('Fixture partition')) : rpc(...args);
      service.handleRpc = (...args: any[]) => isolated.includes(index) ? Promise.reject(Error('Fixture partition')) : handleRpc(...args);
      while (!server.getPort()) await new Promise(resolve => setTimeout(resolve, 10));
      peers[getPublicKey(key)] = `ws://127.0.0.1:${server.getPort()}`;
    }
    client = new ConsensusV2Client({ relayUrl: peers[getPublicKey(keys[0])], guildId, anchorPolicy: anchor,
      readSigner: async payload => ({ author: owner, signature: await sign(ownerKey, hashObject(payload)) }) });
    let previous: GuildEvent | undefined;
    const expectedText = 'Certified history loaded in the actual bundled native application.';
    for (const body of [
      { type: 'GUILD_CREATE', guildId, name: 'Certified native pilot', access: 'public' },
      { type: 'CHANNEL_CREATE', guildId, channelId, name: 'native-validation', kind: 'text' },
      { type: 'MESSAGE', guildId, channelId, messageId: hashObject({ guildId, seed: true }), content: expectedText },
    ]) {
      const createdAt = Date.now();
      const unsigned = { body, author: owner, createdAt };
      const event = { ...unsigned, signature: await sign(ownerKey, hashObject(unsigned)), seq: previous ? previous.seq + 1 : 0, prevHash: previous?.id ?? null } as GuildEvent;
      event.id = computeEventId(event);
      const result = await client.submitEvent(event);
      if (!result.requestedCommitted) throw Error('Fixture seed was not committed');
      previous = event;
    }
    fs.writeFileSync(path.join(directory, 'fixture.json'), JSON.stringify({ pid: process.pid, ports: servers.map(server => server.getPort()),
      channelId, expectedText, trust: { protocol: 'hollow/consensus-trust/2', guildId, anchorPolicy: anchor, relayUrls: Object.values(peers) } }, null, 2));
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Fixture failed';
    console.error(message);
    fs.writeFileSync(path.join(directory, 'failure.json'), JSON.stringify({ message, at: new Date().toISOString() }));
    await close(1);
  }
}
void main().catch(error => { console.error(error instanceof Error ? error.message : 'Fixture failed'); process.exitCode = 1; });
