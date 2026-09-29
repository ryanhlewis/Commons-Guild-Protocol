import { describe, expect, it } from 'vitest';
import { WebSocketServer } from 'ws';
import { ConsensusV2Client } from '@cgp/client';
import {
  generatePrivateKey, getPublicKey, sign, hashObject, computeEventId,
  consensusPolicyHash, consensusUnsigned, verifyObject,
  type ConsensusHistory, type ConsensusPolicy, type ConsensusCommit, type GuildEvent,
} from '@cgp/core';

async function fixture() {
  const keys = Array.from({ length: 3 }, () => generatePrivateKey());
  const authorKey = generatePrivateKey(), author = getPublicKey(authorKey), guildId = 'v2-client-guild';
  const anchor: ConsensusPolicy = { epoch: 'first', members: keys.map(getPublicKey).sort(), requiredVotes: 2, administrators: [author], requiredAdministrators: 1 };
  async function history(text = 'one'): Promise<ConsensusHistory> {
    const body = { type: 'GUILD_CREATE', guildId, name: text } as GuildEvent['body'];
    const event = { seq: 0, prevHash: null, body, author, createdAt: 100, signature: await sign(authorKey, hashObject({ body, author, createdAt: 100 })) } as GuildEvent;
    event.id = computeEventId(event);
    const commit: ConsensusCommit = { protocol: 'cgp/consensus-commit/2', guildId, index: 0, parentHash: null, policyHash: consensusPolicyHash(anchor), ballot: { counter: 1, proposer: anchor.members[0] }, value: { kind: 'event', event }, votes: [] };
    for (const key of keys.slice(0, 2)) {
      const vote = { protocol: 'cgp/consensus-vote/2' as const, guildId, index: 0, parentHash: null, policyHash: commit.policyHash, ballot: commit.ballot, relayPublicKey: getPublicKey(key), valueHash: hashObject(commit.value), signature: '' };
      vote.signature = await sign(key, hashObject(consensusUnsigned(vote)));
      commit.votes.push(vote);
    }
    return { protocol: 'cgp/consensus-history/2', guildId, anchorPolicy: structuredClone(anchor), entries: [{ commit }] };
  }
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
  await new Promise<void>(resolve => server.once('listening', resolve));
  let respond: (kind: string, request: any, send: (kind: string, payload: unknown) => void) => void = () => {};
  const requests: any[] = [];
  server.on('connection', socket => socket.on('message', raw => {
    const [kind, request] = JSON.parse(raw.toString());
    if (kind === 'HELLO') return;
    requests.push(request);
    respond(kind, request, (name, payload) => socket.send(JSON.stringify([name, payload])));
  }));
  const client = new ConsensusV2Client({ relayUrl: `ws://127.0.0.1:${(server.address() as any).port}`, guildId, anchorPolicy: anchor, timeoutMs: 300,
    readSigner: async payload => ({ author, signature: await sign(authorKey, hashObject(payload)) }) });
  return { client, anchor, guildId, history, requests,
    setResponder(fn: typeof respond) { respond = fn; },
    async close() { client.close(); for (const socket of server.clients) socket.terminate(); await new Promise<void>(resolve => server.close(() => resolve())); } };
}

describe('opt-in consensus v2 client', () => {
  it('uses signed correlated reads and verifies a caller-pinned certificate chain', async () => {
    const f = await fixture();
    try {
      const history = await f.history();
      f.setResponder((_, request, send) => {
        expect(verifyObject(request.author, { protocol: 'cgp/consensus-read/2', guildId: request.guildId, requestId: request.requestId, createdAt: request.createdAt }, request.signature)).toBe(true);
        send('CONSENSUS_RESULT', { requestId: 'unrelated', guildId: f.guildId, history: {} });
        send('CONSENSUS_RESULT', { requestId: request.requestId, guildId: f.guildId, history });
      });
      const result = await f.client.fetchHistory();
      expect(result.verified.events).toHaveLength(1);
      result.history.entries.length = 0;
      expect((await f.client.fetchHistory()).history.entries).toHaveLength(1);
    } finally { await f.close(); }
  });

  it('rejects substituted anchors, forged votes and certified prefix forks', async () => {
    const f = await fixture();
    try {
      let history = await f.history();
      f.setResponder((_, request, send) => send('CONSENSUS_RESULT', { requestId: request.requestId, guildId: f.guildId, history }));
      const valid = structuredClone(history);
      history.anchorPolicy.epoch = 'attacker';
      await expect(f.client.fetchHistory()).rejects.toThrow('anchor');
      history = structuredClone(valid); history.entries[0].commit.votes[0].signature = '00'.repeat(64);
      await expect(f.client.fetchHistory()).rejects.toThrow('certificate');
      history = valid; await f.client.fetchHistory();
      history = await f.history('conflicting but signed');
      await expect(f.client.fetchHistory()).rejects.toThrow('fork');
    } finally { await f.close(); }
  });

  it('rejects rollback and does not equate recovered older value with submitted value', async () => {
    const f = await fixture();
    try {
      let history = await f.history();
      f.setResponder((_, request, send) => send('CONSENSUS_RESULT', { requestId: request.requestId, guildId: f.guildId, history }));
      await f.client.fetchHistory();
      const different = (await f.history('requested')).entries[0].commit.value;
      if (different.kind !== 'event') throw Error('fixture');
      const result = await f.client.submitEvent(different.event);
      expect(result.requestedCommitted).toBe(false);
      expect(result.appended).toHaveLength(0);
      history = { ...history, entries: [] };
      await expect(f.client.fetchHistory()).rejects.toThrow('rollback');
    } finally { await f.close(); }
  });

  it('bounds lost responses and rejects pending work when closed', async () => {
    const f = await fixture();
    try {
      await expect(f.client.fetchHistory()).rejects.toThrow('outcome is unknown');
      const request = f.client.fetchHistory();
      await new Promise(resolve => setTimeout(resolve, 20));
      f.client.close();
      await expect(request).rejects.toThrow('closed');
    } finally { await f.close(); }
  });

  it('returns certified duplicate event retries without proposing again and rejects spoofed IDs', async () => {
    const f = await fixture();
    try {
      const history = await f.history();
      let publishes = 0;
      f.setResponder((kind, request, send) => {
        if (kind === 'CONSENSUS_PUBLISH') publishes++;
        send('CONSENSUS_RESULT', { requestId: request.requestId, guildId: f.guildId, history });
      });
      const value = history.entries[0].commit.value;
      if (value.kind !== 'event') throw Error('fixture');
      expect((await f.client.submitEvent(value.event)).requestedCommitted).toBe(true);
      expect(publishes).toBe(0);
      const forged = structuredClone(value.event);
      forged.createdAt++;
      expect((await f.client.submitEvent(forged)).requestedCommitted).toBe(false);
      expect(publishes).toBe(1);
    } finally { await f.close(); }
  });

  it('does not acknowledge import when the relay returns an older valid prefix', async () => {
    const f = await fixture();
    try {
      const candidate = await f.history();
      const empty = { ...candidate, entries: [] };
      f.setResponder((_, request, send) => send('CONSENSUS_RESULT', { requestId: request.requestId, guildId: f.guildId, history: empty }));
      await f.client.fetchHistory();
      await expect(f.client.importHistory(candidate)).rejects.toThrow('rollback');
    } finally { await f.close(); }
  });
});
