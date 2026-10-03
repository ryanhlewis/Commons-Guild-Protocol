import { randomUUID } from 'node:crypto';
import { WebSocket } from 'ws';
import { hashObject, sign, verify, computeEventId, consensusUnsigned, consensusCommitHash, consensusMigrationHash, verifyConsensusHistory, validConsensusValue,
  type ConsensusPolicy, type ConsensusValue, type ConsensusHistory, type LegacyConsensusTrust } from '@cgp/core';
import { RelayConsensusCoordinator, type ConsensusStateStore, type ConsensusCallbacks } from './consensus_v2';

export interface RelayConsensusConfig {
  /** Explicit trust roots, provisioned independently of HELLO and peer discovery. */
  guilds: Record<string, ConsensusPolicy>;
  /** Addresses are transport hints only; every RPC response is identity authenticated. */
  peers: Record<string, string>;
  legacyTrust?: Record<string, LegacyConsensusTrust>;
  timeoutMs?: number;
}
type Method = 'prepare' | 'accept' | 'commit' | 'sync' | 'ready' | 'activate' | 'history' | 'status';
const methods = new Set<Method>(['prepare', 'accept', 'commit', 'sync', 'ready', 'activate', 'history', 'status']);
interface Rpc {
  protocol: 'cgp/consensus-rpc/2'; requestId: string; guildId: string;
  method: Method; payload?: unknown; createdAt: number; relayPublicKey: string; signature: string;
}

/** Opt-in transport. Legacy guilds never enter this service. */
export class RelayConsensusService {
  private coordinators = new Map<string, RelayConsensusCoordinator>();
  private sockets = new Set<WebSocket>();
  private proposals = new Map<string, Promise<unknown>>();
  private closed = false;
  constructor(readonly config: RelayConsensusConfig,
    private identity: { publicKey: string; privateKey: Uint8Array },
    private store: ConsensusStateStore,
    private callbacks: (guildId: string) => ConsensusCallbacks) {}

  has(guildId: unknown): guildId is string {
    return typeof guildId === 'string' && Object.prototype.hasOwnProperty.call(this.config.guilds, guildId);
  }
  coordinator(guildId: string) {
    if (this.closed) throw Error('Consensus service is closed');
    if (!this.has(guildId)) throw Error('Guild has no pinned consensus trust root');
    let coordinator = this.coordinators.get(guildId);
    if (!coordinator) {
      coordinator = new RelayConsensusCoordinator(guildId, this.config.guilds[guildId], this.identity, this.store, this.callbacks(guildId), this.config.legacyTrust?.[guildId]);
      this.coordinators.set(guildId, coordinator);
    }
    return coordinator;
  }
  async invoke(guildId: string, method: Method, payload?: unknown): Promise<unknown> {
    if (this.closed) throw Error('Consensus service is closed');
    if (!methods.has(method)) throw Error('Unsupported consensus RPC');
    const coordinator = this.coordinator(guildId);
    // Method whitelist above is deliberately separate from object property lookup.
    switch (method) {
      case 'prepare': return coordinator.prepare(payload as Parameters<typeof coordinator.prepare>[0]);
      case 'accept': return coordinator.accept(payload as Parameters<typeof coordinator.accept>[0]);
      case 'commit': await coordinator.commit(payload as Parameters<typeof coordinator.commit>[0]); return true;
      case 'sync': await coordinator.sync(payload as ConsensusHistory); return true;
      case 'activate': await coordinator.activate(payload as Parameters<typeof coordinator.activate>[0]); return true;
      case 'ready': return coordinator.ready();
      case 'history': return coordinator.history();
      case 'status': return coordinator.status();
    }
  }
  async handleRpc(request: Rpc) {
    if (!request || request.protocol !== 'cgp/consensus-rpc/2' || typeof request.requestId !== 'string' || request.requestId.length > 100 ||
        !methods.has(request.method) || !Number.isFinite(request.createdAt) || Math.abs(Date.now() - request.createdAt) > 60_000 ||
        !verify(request.relayPublicKey, hashObject(consensusUnsigned(request)), request.signature)) throw Error('Invalid consensus RPC authentication');
    const coordinator = this.coordinator(request.guildId);
    const history = await coordinator.history();
    const checked = verifyConsensusHistory(history, request.guildId, coordinator.anchor, coordinator.legacyTrust);
    const nextMembers = checked.pendingTransition?.value.kind === 'transition' ? checked.pendingTransition.value.nextPolicy.members : [];
    let authorized = checked.policy.members.includes(request.relayPublicKey) || nextMembers.includes(request.relayPublicKey);
    if (!authorized && request.method === 'sync') {
      // A lagging new voter can learn the certified transition after the old
      // operators leave. The supplied chain must validate against OUR anchor.
      const incoming = verifyConsensusHistory(request.payload as ConsensusHistory, request.guildId, coordinator.anchor, coordinator.legacyTrust);
      const incomingNext = incoming.pendingTransition?.value.kind === 'transition' ? incoming.pendingTransition.value.nextPolicy.members : [];
      authorized = incoming.policy.members.includes(request.relayPublicKey) || incomingNext.includes(request.relayPublicKey);
    }
    if (!authorized) throw Error('RPC caller is not an authorized epoch voter');
    let result: unknown, error: string | undefined;
    try { result = await this.invoke(request.guildId, request.method, request.payload); }
    catch (cause) { error = cause instanceof Error ? cause.message : 'Consensus operation failed'; }
    const unsigned = { protocol: 'cgp/consensus-rpc-result/2', requestId: request.requestId,
      guildId: request.guildId, relayPublicKey: this.identity.publicKey, result: result ?? null, error: error ?? null };
    return { ...unsigned, signature: await sign(this.identity.privateKey, hashObject(unsigned)) };
  }
  async rpc(guildId: string, peer: string, method: Method, payload?: unknown): Promise<unknown> {
    if (this.closed) throw Error('Consensus service is closed');
    if (peer === this.identity.publicKey) return this.invoke(guildId, method, payload);
    const url = this.config.peers[peer];
    if (!url) throw Error('No configured address for consensus voter');
    const unsigned = { protocol: 'cgp/consensus-rpc/2' as const, requestId: randomUUID(), guildId, method,
      ...(payload === undefined ? {} : { payload }), createdAt: Date.now(), relayPublicKey: this.identity.publicKey };
    const request = { ...unsigned, signature: await sign(this.identity.privateKey, hashObject(unsigned)) };
    return new Promise((resolve, reject) => {
      const socket = new WebSocket(url, { maxPayload: 16 * 1024 * 1024 });
      this.sockets.add(socket);
      let settled = false;
      const finish = (error?: Error, value?: unknown) => {
        if (settled) return;
        settled = true; clearTimeout(timer); this.sockets.delete(socket); socket.terminate();
        error ? reject(error) : resolve(value);
      };
      const timer = setTimeout(() => finish(Error('Consensus RPC timed out')), this.config.timeoutMs ?? 5000);
      socket.on('error', error => finish(error));
      socket.on('close', () => finish(Error('Consensus RPC closed before response')));
      socket.on('open', () => socket.send(JSON.stringify(['CONSENSUS_RPC', request])));
      socket.on('message', data => {
        try {
          const [kind, response] = JSON.parse(data.toString());
          if (kind !== 'CONSENSUS_RPC_RESULT' || response.requestId !== request.requestId) return;
          if (response.protocol !== 'cgp/consensus-rpc-result/2' || response.guildId !== guildId || response.relayPublicKey !== peer ||
              !verify(peer, hashObject(consensusUnsigned(response)), response.signature)) throw Error('Invalid consensus RPC response');
          finish(response.error ? Error(response.error) : undefined, response.result);
        } catch (error) { finish(error instanceof Error ? error : Error('Invalid consensus response')); }
      });
    });
  }
  async propose(guildId: string, value: ConsensusValue) {
    if (this.closed) throw Error('Consensus service is closed');
    if (this.proposals.has(guildId)) throw Error('Consensus proposal already in progress; fetch history before retrying');
    const previous = this.proposals.get(guildId) ?? Promise.resolve();
    const operation = previous.catch(() => undefined).then(async () => {
      const coordinator = this.coordinator(guildId);
      const transport = { call: (peer: string, method: Method, payload: unknown) => this.rpc(guildId, peer, method, payload) };
      const initial = await coordinator.status();
      const histories = await Promise.allSettled(initial.policy.members.map(peer => this.rpc(guildId, peer, 'history')));
      const candidates: ConsensusHistory[] = [];
      for (const response of histories) if (response.status === 'fulfilled') {
        const history = response.value as ConsensusHistory;
        verifyConsensusHistory(history, guildId, coordinator.anchor, coordinator.legacyTrust);
        candidates.push(history);
      }
      candidates.push(await coordinator.history());
      candidates.sort((a, b) => b.entries.length - a.entries.length ||
        Number(Boolean(b.base)) - Number(Boolean(a.base)) ||
        b.entries.filter(entry => entry.activation).length - a.entries.filter(entry => entry.activation).length);
      const best = candidates[0];
      for (const history of candidates) {
        if (history.base && best.base && consensusMigrationHash(history.base) !== consensusMigrationHash(best.base)) throw Error('Conflicting certified migration bases');
        for (let index = 0; index < history.entries.length; index++) {
          if (consensusCommitHash(history.entries[index].commit) !== consensusCommitHash(best.entries[index].commit)) throw Error('Conflicting certified histories');
        }
      }
      await coordinator.sync(best);
      const status = await coordinator.status();
      if (status.pendingTransition) {
        await coordinator.activateTransition(transport);
        return coordinator.history();
      }
      const checked = verifyConsensusHistory(await coordinator.history(), guildId, coordinator.anchor, coordinator.legacyTrust);
      if ((value?.kind === 'event' && value.event.id === computeEventId(value.event) && checked.events.some(event => event.id === value.event.id)) ||
          best.entries.some(entry => hashObject(entry.commit.value) === hashObject(value))) return coordinator.history();
      if (!validConsensusValue(value, checked.scope, checked.policy) ||
          (value.kind === 'event' && !await this.callbacks(guildId).validateEvent(value.event, checked.events, 'accept'))) {
        throw Error('Invalid or unauthorized consensus proposal');
      }
      await Promise.allSettled(status.policy.members.filter(peer => peer !== this.identity.publicKey).map(peer => this.rpc(guildId, peer, 'sync', best)));
      const statuses = await Promise.allSettled(status.policy.members.map(peer => this.rpc(guildId, peer, 'status')));
      let counter = status.promised?.counter ?? 0;
      for (const response of statuses) if (response.status === 'fulfilled') {
        const remote = response.value as { promised?: { counter?: number } };
        if (Number.isSafeInteger(remote.promised?.counter)) counter = Math.max(counter, remote.promised!.counter!);
      }
      const result = await coordinator.propose(value, transport, counter);
      if (result.commit.value.kind === 'transition') await coordinator.activateTransition(transport);
      return coordinator.history();
    });
    this.proposals.set(guildId, operation);
    try { return await operation; }
    finally { if (this.proposals.get(guildId) === operation) this.proposals.delete(guildId); }
  }
  async close() {
    this.closed = true;
    for (const socket of this.sockets) socket.terminate();
    this.sockets.clear();
    await Promise.allSettled(this.proposals.values());
    // Drain acceptor operations before the owning server closes its store.
    await Promise.allSettled([...this.coordinators.values()].map(coordinator => coordinator.history()));
  }
}
