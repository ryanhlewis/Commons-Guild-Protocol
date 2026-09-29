import { expect, it } from 'vitest';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { WebSocket } from 'ws';
import { RelayServer, LocalRelayPubSubAdapter } from '@cgp/relay/src/server';
import { ConsensusV2Client } from '@cgp/client';
import {
  generatePrivateKey, getPublicKey, hashObject, sign, computeEventId,
  createRelayWriteCertificate, relayWriteProposalId, consensusPolicyHash,
  legacyConsensusPolicyHash, legacyConsensusRequestPayload, verifyConsensusHistory,
  type ConsensusPolicy, type ConsensusHistory, type LegacyConsensusTrust,
  type LegacyConsensusMigrationRequest, type RelayWriteProposal, type GuildEvent,
} from '@cgp/core';

it('migrates a certified legacy prefix with partial fences and keeps retirement after a legacy-only restart', async () => {
  const prefix = path.join(os.tmpdir(), 'cgp-legacy-live-');
  const directory = await fs.mkdtemp(prefix);
  const keys = Array.from({ length: 3 }, () => generatePrivateKey());
  const members = keys.map(getPublicKey), ownerKey = generatePrivateKey(), owner = getPublicKey(ownerKey);
  const guildId = 'legacy-live-synthetic';
  const legacy = { epoch: 'legacy-live', members, requiredVotes: 2 };
  const trust: LegacyConsensusTrust = { policy: legacy, administrators: [owner], requiredAdministrators: 1 };
  const anchor: ConsensusPolicy = { ...legacy, epoch: 'migrated-live', administrators: [owner], requiredAdministrators: 1 };
  const bus = new LocalRelayPubSubAdapter();
  let isolated = false;
  const peers: Record<string, string> = {};
  const servers: Array<RelayServer | undefined> = [];
  let client: ConsensusV2Client | undefined;
  async function start(index: number, v2: boolean) {
    const server = new RelayServer(0, path.join(directory, `node-${index}`), [], {
      listenHost: '127.0.0.1', enableDefaultPlugins: false, sequencerConsensus: false,
      relayPrivateKeyHex: Buffer.from(keys[index]).toString('hex'),
      pubSubAdapter: {
        isReady: () => true,
        publish: (topic, envelope) => { if (!isolated) return bus.publish(topic, envelope); },
        subscribe: (topic, handler) => bus.subscribe(topic, handler),
      },
      writeQuorum: { ...legacy, voteTimeoutMs: 120 },
      ...(v2 ? { consensusV2: { guilds: { [guildId]: anchor }, legacyTrust: { [guildId]: trust }, peers, timeoutMs: 1500 } } : {}),
    });
    servers[index] = server;
    const http = (server as any).httpServer;
    if (!http.listening) await new Promise<void>(resolve => http.once('listening', resolve));
    peers[members[index]] = `ws://127.0.0.1:${http.address().port}`;
  }
  async function restart(index: number, v2: boolean) {
    await servers[index]!.close(); servers[index] = undefined; await start(index, v2);
  }
  async function proposal(name: string, head?: GuildEvent): Promise<RelayWriteProposal> {
    const body = head
      ? { type: 'CHANNEL_CREATE' as const, guildId, channelId: name, name, kind: 'text' as const }
      : { type: 'GUILD_CREATE' as const, guildId, name, access: 'private' as const };
    const createdAt = Date.now(), signature = await sign(ownerKey, hashObject({ body, author: owner, createdAt }));
    return { guildId, headSeq: head?.seq ?? -1, headHash: head?.id ?? null, body, author: owner, createdAt, signature, clientEventId: name };
  }
  function event(p: RelayWriteProposal): GuildEvent {
    const value = { body: p.body, author: p.author, createdAt: p.createdAt, signature: p.signature, seq: p.headSeq + 1, prevHash: p.headHash } as GuildEvent;
    return { ...value, id: computeEventId(value) };
  }
  async function operatorRpc(index: number, kind: string, extra: Record<string, unknown>, key = ownerKey) {
    const socket = new WebSocket(peers[members[index]]);
    try {
      await new Promise<void>((resolve, reject) => { socket.once('open', resolve); socket.once('error', reject); });
      const requestId = randomUUID(), createdAt = Date.now();
      const signed = { protocol: 'cgp/consensus-read/2', guildId, requestId, createdAt };
      const payload = { guildId, requestId, createdAt, author: getPublicKey(key), signature: await sign(key, hashObject(signed)), ...extra };
      return await new Promise<{ kind: string; payload: any }>((resolve, reject) => {
        const timer = setTimeout(() => reject(Error('Operator RPC timed out')), 5000);
        socket.on('message', data => {
          const [responseKind, response] = JSON.parse(data.toString());
          if (response?.requestId !== requestId) return;
          clearTimeout(timer); resolve({ kind: responseKind, payload: response });
        });
        socket.once('error', error => { clearTimeout(timer); reject(error); });
        socket.send(JSON.stringify([kind, payload]));
      });
    } finally { socket.terminate(); }
  }
  try {
    for (let i = 0; i < 3; i++) await start(i, false);
    const genesisProposal = await proposal('legacy genesis'), genesis = event(genesisProposal);
    const proposalId = relayWriteProposalId(legacy.epoch, genesisProposal);
    const votes = await Promise.all(keys.slice(0, 2).map(async key => {
      const unsigned = { protocol: 'cgp/write-vote/1' as const, epoch: legacy.epoch, relayPublicKey: getPublicKey(key), guildId,
        headSeq: -1, headHash: null, proposalId, votedAt: Date.now() };
      return { ...unsigned, signature: await sign(key, hashObject(unsigned)) };
    }));
    genesis.writeCertificate = createRelayWriteCertificate(legacy, genesisProposal, votes);
    // Synthetic certificate setup goes through the real legacy ingress validator.
    for (const server of servers) expect(await (server as any).replicatePubSubEvents(guildId, [genesis])).toHaveLength(1);
    isolated = true;
    for (let i = 0; i < 3; i++) {
      await expect((servers[i] as any).writeQuorumCoordinator.authorize(await proposal(`partial-${i}`, genesis))).rejects.toThrow('quorum unavailable');
    }
    for (let i = 0; i < 3; i++) await restart(i, true);
    const unsigned = { protocol: 'cgp/legacy-migration-request/2' as const, guildId,
      legacyPolicyHash: legacyConsensusPolicyHash(trust), nextPolicyHash: consensusPolicyHash(anchor), nonce: 'live-migration' };
    const request: LegacyConsensusMigrationRequest = { ...unsigned,
      signatures: [{ publicKey: owner, signature: await sign(ownerKey, hashObject(legacyConsensusRequestPayload({ ...unsigned, signatures: [] }))) }] };
    expect((await operatorRpc(0, 'CONSENSUS_FREEZE', { migration: request }, generatePrivateKey())).kind).toBe('CONSENSUS_ERROR');
    expect(await (servers[0] as any).store.getLegacyConsensusFreeze(guildId)).toBeUndefined();
    client = new ConsensusV2Client({ relayUrl: peers[members[0]], guildId, anchorPolicy: anchor, legacyTrust: trust,
      readSigner: async payload => ({ author: owner, signature: await sign(ownerKey, hashObject(payload)) }) });
    const initial = await client.fetchHistory();
    expect(initial.history.entries).toHaveLength(0);
    expect(initial.history.base).toBeUndefined();
    const frozen = await client.freezeLegacy(request, members[0]);
    const freezes = [frozen, ...await Promise.all(servers.slice(1).map(server => server!.freezeLegacyConsensus(request)))];
    expect(new Set(freezes.map(freeze => freeze.fenceProposalId)).size).toBe(3);
    const history: ConsensusHistory = { protocol: 'cgp/consensus-history/2', guildId, anchorPolicy: anchor,
      base: { protocol: 'cgp/legacy-bridge/2', request, freezes, events: [genesis] }, entries: [] };
    expect(verifyConsensusHistory(history, guildId, anchor, trust).events.map(item => item.id)).toEqual([genesis.id]);
    expect((await operatorRpc(0, 'CONSENSUS_IMPORT', { history }, generatePrivateKey())).kind).toBe('CONSENSUS_ERROR');
    const imported = await client.importHistory(history);
    expect(imported.history.base).toEqual(history.base);
    expect(imported.verified.events.map(item => item.id)).toEqual([genesis.id]);
    for (const server of servers.slice(1)) await server!.importConsensusHistory(history);
    client.close(); client = undefined;

    // Removing the v2 configuration must not remove durable retirement.
    await restart(0, false);
    const late = await proposal('late-legacy', genesis);
    await expect((servers[0] as any).writeQuorumCoordinator.authorize(late)).rejects.toThrow('durably frozen');
    const lateEvent = event(late), lateId = relayWriteProposalId(legacy.epoch, late);
    const lateVotes = await Promise.all(keys.slice(0, 2).map(async key => {
      const unsigned = { protocol: 'cgp/write-vote/1' as const, epoch: legacy.epoch, relayPublicKey: getPublicKey(key), guildId,
        headSeq: genesis.seq, headHash: genesis.id, proposalId: lateId, votedAt: Date.now() };
      return { ...unsigned, signature: await sign(key, hashObject(unsigned)) };
    }));
    lateEvent.writeCertificate = createRelayWriteCertificate(legacy, late, lateVotes);
    await expect((servers[0] as any).replicatePubSubEvents(guildId, [lateEvent])).rejects.toThrow('retired');
    expect((await (servers[0] as any).store.getLog(guildId)).map((item: GuildEvent) => item.id)).toEqual([genesis.id]);
    const record = await (servers[0] as any).store.getLegacyConsensusFreeze(guildId);
    expect(record.freeze).toEqual(freezes[0]);
    const fenceKey = hashObject({ protocol: 'cgp/write-fence/1', epoch: legacy.epoch, guildId, headSeq: genesis.seq, headHash: genesis.id });
    expect(await (servers[0] as any).store.getWriteVoteFence(fenceKey)).toBe(freezes[0].fenceProposalId);
    await restart(0, true);
    expect(await servers[0]!.freezeLegacyConsensus(request)).toEqual(freezes[0]);
    isolated = false;
    client = new ConsensusV2Client({ relayUrl: peers[members[0]], guildId, anchorPolicy: anchor, legacyTrust: trust,
      readSigner: async payload => ({ author: owner, signature: await sign(ownerKey, hashObject(payload)) }) });
    const continued = event(await proposal('v2-after-migration', genesis));
    const result = await client.submitEvent(continued);
    expect(result.requestedCommitted).toBe(true);
    expect(result.verified.events.map(item => item.id)).toEqual([genesis.id, continued.id]);
    expect(result.verified.events.at(-1)?.seq).toBe(1);
  } finally {
    client?.close();
    await Promise.all(servers.map(server => server?.close()));
    const resolved = path.resolve(directory);
    if (!resolved.startsWith(path.resolve(prefix))) throw Error('Unsafe fixture cleanup path');
    await fs.rm(resolved, { recursive: true, force: true });
  }
}, 45000);
