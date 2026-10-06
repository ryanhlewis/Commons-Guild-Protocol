import { it, expect } from 'vitest';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { WebSocket } from 'ws';
import { ConsensusV2Client } from '@cgp/client';
import { RelayServer } from '@cgp/relay';
import {
  generatePrivateKey, getPublicKey, hashObject, sign, computeEventId,
  consensusPolicyHash, consensusTransitionPayload,
  type ConsensusPolicy, type ConsensusTransition, type GuildEvent, type EventBody,
} from '@cgp/core';

it('certifies WebSocket/LevelDB recovery, membership changes and new-voter bootstrap without bypasses', async () => {
  const prefix = path.join(os.tmpdir(), 'cgp-consensus-v2-live-');
  const directory = await fs.mkdtemp(prefix);
  const keys = Array.from({ length: 5 }, () => generatePrivateKey());
  const voterKeys = keys.map(getPublicKey), adminKey = generatePrivateKey(), admin = getPublicKey(adminKey);
  const guildId = 'live-private-v2';
  const anchor: ConsensusPolicy = { epoch: 'abc', members: voterKeys.slice(0, 3).sort(), requiredVotes: 2, administrators: [admin], requiredAdministrators: 1 };
  const peers: Record<string, string> = {};
  const servers: Array<RelayServer | undefined> = [];
  const clients: ConsensusV2Client[] = [];
  async function start(index: number) {
    const server = new RelayServer(0, path.join(directory, `node-${index}`), [], {
      listenHost: '127.0.0.1', enableDefaultPlugins: false,
      relayPrivateKeyHex: Buffer.from(keys[index]).toString('hex'),
      consensusV2: { guilds: { [guildId]: anchor }, peers, timeoutMs: 1500 },
    });
    servers[index] = server;
    const http = (server as any).httpServer;
    if (!http.listening) await new Promise<void>(resolve => http.once('listening', resolve));
    peers[voterKeys[index]] = `ws://127.0.0.1:${http.address().port}`;
  }
  function client(index: number, key = adminKey) {
    const c = new ConsensusV2Client({ relayUrl: peers[voterKeys[index]], guildId, anchorPolicy: anchor, timeoutMs: 15000,
      readSigner: async payload => ({ author: getPublicKey(key), signature: await sign(key, hashObject(payload)) }) });
    clients.push(c); return c;
  }
  async function event(body: EventBody, seq: number, prevHash: string | null): Promise<GuildEvent> {
    const createdAt = Date.now(), signature = await sign(adminKey, hashObject({ body, author: admin, createdAt }));
    const value = { body, author: admin, createdAt, signature, seq, prevHash } as GuildEvent;
    value.id = computeEventId(value); return value;
  }
  try {
    for (let i = 0; i < 5; i++) await start(i);
    const a = client(0);
    const genesis = await event({ type: 'GUILD_CREATE', guildId, name: 'private', access: 'private' }, 0, null);
    expect((await a.submitEvent(genesis)).requestedCommitted).toBe(true);
    await expect(client(1, generatePrivateKey()).fetchHistory()).rejects.toThrow('access denied');

    const socket = new WebSocket(peers[voterKeys[0]]);
    await new Promise<void>(resolve => socket.once('open', resolve));
    const legacyResult = new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(Error('Legacy refusal timed out')), 1500);
      socket.on('message', data => { const [kind, payload] = JSON.parse(data.toString()); if (kind === 'ERROR') { clearTimeout(timer); resolve(payload); } });
    });
    socket.send(JSON.stringify(['PUBLISH', { body: genesis.body, author: admin, signature: genesis.signature, createdAt: genesis.createdAt }]));
    expect((await legacyResult).code).toBe('CONSENSUS_V2_REQUIRED');
    socket.terminate();

    const selected = await event({ type: 'CHANNEL_CREATE', guildId, channelId: 'selected', name: 'selected', kind: 'text' }, 1, genesis.id);
    const competitor = await event({ type: 'CHANNEL_CREATE', guildId, channelId: 'competitor', name: 'competitor', kind: 'text' }, 1, genesis.id);
    const service = (servers[0] as any).consensusV2;
    const coordinator = service.coordinator(guildId);
    const prepare = await coordinator.nextPrepareRequest();
    const promises = await Promise.all(voterKeys.slice(0, 3).map(peer => service.rpc(guildId, peer, 'prepare', prepare)));
    const unsigned = { protocol: 'cgp/consensus-accept/2', guildId, index: prepare.index, parentHash: prepare.parentHash,
      policyHash: prepare.policyHash, ballot: prepare.ballot, value: { kind: 'event', event: selected }, promises };
    const accept = { ...unsigned, signature: await sign(keys[0], hashObject(unsigned)) };
    await Promise.all(voterKeys.slice(0, 2).map(peer => service.rpc(guildId, peer, 'accept', accept)));
    // Accepted on a quorum but never committed/broadcast: both durable voters restart.
    a.close();
    for (const i of [0, 1]) { await servers[i]!.close(); servers[i] = undefined; await start(i); }
    const c = client(2);
    const recovered = await c.submitEvent(competitor).catch(error => { throw Error(`Recovery publish: ${error.message}`); });
    expect(recovered.requestedCommitted).toBe(false);
    expect(recovered.verified.events.at(-1)?.id).toBe(selected.id);

    const nextPolicy: ConsensusPolicy = { ...anchor, epoch: 'bcd', members: voterKeys.slice(1, 4).sort() };
    const transition: ConsensusTransition = { kind: 'transition', guildId,
      fromPolicyHash: consensusPolicyHash(anchor), parentHash: recovered.verified.scope.parentHash,
      nextPolicy, nonce: 'live-transition', signatures: [] };
    transition.signatures.push({ publicKey: admin, signature: await sign(adminKey, hashObject(consensusTransitionPayload(transition))) });
    const cService = (servers[2] as any).consensusV2;
    const originalRpc = cService.rpc.bind(cService);
    // B remains reachable but misses final activation after the new majority is ready.
    cService.rpc = (guild: string, peer: string, method: string, payload: unknown) =>
      peer === voterKeys[1] && method === 'activate'
        ? Promise.reject(Error('fixture dropped activation'))
        : originalRpc(guild, peer, method, payload);
    let moved;
    try { moved = await c.submitTransition(transition).catch(error => { throw Error(`Transition publish: ${error.message}`); }); }
    finally { cService.rpc = originalRpc; }
    expect(moved.requestedCommitted).toBe(true);
    expect(moved.verified.policy.epoch).toBe('bcd');
    expect(moved.verified.pendingTransition).toBeUndefined();
    expect((await (servers[1] as any).consensusV2.coordinator(guildId).status()).pendingTransition).not.toBeNull();
    const d = client(3);
    await d.fetchHistory().catch(error => { throw Error(`New voter history: ${error.message}`); });
    const after = await event({ type: 'CHANNEL_CREATE', guildId, channelId: 'after', name: 'after', kind: 'text' }, 2, selected.id);
    expect((await d.submitEvent(after).catch(error => { throw Error(`New voter publish: ${error.message}`); })).requestedCommitted).toBe(true);
    expect((await (servers[1] as any).consensusV2.coordinator(guildId).status()).pendingTransition).toBeNull();
    await expect((servers[0] as any).consensusV2.coordinator(guildId).nextPrepareRequest()).rejects.toThrow();
    d.close(); await servers[3]!.close(); servers[3] = undefined; await start(3);
    expect((await client(3).fetchHistory()).verified.events.map(item => item.id)).toEqual([genesis.id, selected.id, after.id]);

    // E misses the entire CDE transition. Only C+D readiness is available.
    // D was not in E's original ABC anchor, so bootstrap must authenticate
    // D against the certified incoming history, never an unverified HELLO.
    const latest = await c.fetchHistory();
    const cde: ConsensusTransition = { kind: 'transition', guildId, fromPolicyHash: consensusPolicyHash(nextPolicy),
      parentHash: latest.verified.scope.parentHash, nextPolicy: { ...anchor, epoch: 'cde', members: voterKeys.slice(2).sort() }, nonce: 'bootstrap-e', signatures: [] };
    cde.signatures.push({ publicKey: admin, signature: await sign(adminKey, hashObject(consensusTransitionPayload(cde))) });
    cService.rpc = (guild: string, peer: string, method: string, payload: unknown) =>
      peer === voterKeys[4] ? Promise.reject(Error('fixture E missed transition')) : originalRpc(guild, peer, method, payload);
    let activated;
    try { activated = await c.submitTransition(cde); }
    finally { cService.rpc = originalRpc; }
    expect(activated.verified.policy.epoch).toBe('cde');
    expect((await (servers[4] as any).consensusV2.coordinator(guildId).history()).entries).toHaveLength(0);
    await servers[0]!.close(); servers[0] = undefined;
    const newDService = (servers[3] as any).consensusV2;
    await newDService.rpc(guildId, voterKeys[4], 'sync', activated.history);
    const e = client(4);
    expect((await e.fetchHistory()).verified.policy.epoch).toBe('cde');
    const last = await event({ type: 'CHANNEL_CREATE', guildId, channelId: 'bootstrapped', name: 'bootstrapped', kind: 'text' }, 3, after.id);
    expect((await e.submitEvent(last)).requestedCommitted).toBe(true);
  } finally {
    for (const c of clients) c.close();
    await Promise.all(servers.map(server => server?.close()));
    const resolved = path.resolve(directory);
    if (!resolved.startsWith(path.resolve(prefix))) throw Error('Unsafe fixture cleanup path');
    await fs.rm(resolved, { recursive: true, force: true });
  }
}, 45000);
