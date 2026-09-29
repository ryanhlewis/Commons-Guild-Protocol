import {
  consensusCommitHash, consensusPolicyHash, consensusUnsigned, compareConsensusBallots,
  normalizeConsensusPolicy, selectConsensusValue, verifyConsensusActivation,
  verifyConsensusCommit, verifyConsensusHistory, validConsensusBallot, validConsensusValue,
  hashObject, sign, verify,
  type ConsensusPolicy, type ConsensusBallot, type ConsensusAccepted, type ConsensusScope,
  type ConsensusPrepareRequest, type ConsensusAcceptRequest, type ConsensusPromise,
  type ConsensusVote, type ConsensusCommit, type ConsensusReady, type ConsensusActivation,
  type ConsensusHistory, type ConsensusValue, type GuildEvent,
  type LegacyConsensusTrust, consensusMigrationHash,
} from '@cgp/core';

export interface ConsensusPersistentState {
  protocol: 'cgp/consensus-state/2';
  history: ConsensusHistory;
  promised: ConsensusBallot | null;
  accepted: ConsensusAccepted | null;
  proposerCounter?: number;
}
export interface ConsensusTransport {
  call(peerPublicKey: string, method: 'prepare' | 'accept' | 'commit' | 'sync' | 'ready' | 'activate', payload: unknown): Promise<unknown>;
}
export interface ConsensusStateStore {
  getConsensusState(guildId: string): Promise<ConsensusPersistentState | undefined> | ConsensusPersistentState | undefined;
  /** Must atomically persist the whole state durably before resolving. */
  putConsensusState(guildId: string, state: ConsensusPersistentState): Promise<void> | void;
}
export interface ConsensusCallbacks {
  /**
   * Check semantics without appending application state. Replay uses historical
   * event-time rules. Acceptance may durably advance a verified device-authority
   * pin even if the proposal never commits; callers must not roll that pin back
   * to resurrect a revoked device after an ambiguous proposal outcome.
   */
  validateEvent(event: GuildEvent, prefix: GuildEvent[], mode: 'accept' | 'replay'): Promise<boolean> | boolean;
  /** Idempotently reconcile exactly this certified app prefix, including authority state. */
  materialize(events: GuildEvent[]): Promise<void> | void;
}
const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value));
// Store instances own their storage domain (LevelDB also excludes another
// process opening that directory). Share serialization across coordinators.
const storeQueues = new WeakMap<ConsensusStateStore, Map<string, Promise<unknown>>>();

/** Crash-fault Paxos acceptor for one explicitly anchored guild. Not Byzantine consensus. */
export class RelayConsensusCoordinator {
  private state?: ConsensusPersistentState;
  private verifiedHistory?: { fingerprint: string; value: ReturnType<typeof verifyConsensusHistory> };
  private replayFingerprint?: string;
  readonly anchor: ConsensusPolicy;
  constructor(readonly guildId: string, anchor: ConsensusPolicy,
    private identity: { publicKey: string; privateKey: Uint8Array },
    private store: ConsensusStateStore, private callbacks: ConsensusCallbacks,
    readonly legacyTrust?: LegacyConsensusTrust) {
    this.anchor = normalizeConsensusPolicy(anchor);
    if (!guildId) throw Error('Consensus guild scope required');
  }
  private async run<T>(operation: () => Promise<T>, importing = false): Promise<T> {
    let queues = storeQueues.get(this.store);
    if (!queues) { queues = new Map(); storeQueues.set(this.store, queues); }
    const result = (queues.get(this.guildId) ?? Promise.resolve()).catch(() => undefined).then(async () => {
      {
        const loaded = await this.store.getConsensusState(this.guildId);
        const state = loaded ?? { protocol: 'cgp/consensus-state/2' as const,
          history: { protocol: 'cgp/consensus-history/2' as const, guildId: this.guildId, anchorPolicy: this.anchor, entries: [] }, promised: null, accepted: null };
        if (state.protocol !== 'cgp/consensus-state/2') throw Error('Invalid durable consensus state');
        const checked = this.verifyHistory(state.history);
        if (state.promised && !validConsensusBallot(state.promised, checked.policy)) throw Error('Invalid durable promise');
        if (state.accepted && (!state.promised || !validConsensusBallot(state.accepted.ballot, checked.policy) || compareConsensusBallots(state.accepted.ballot, state.promised) > 0 || !validConsensusValue(state.accepted.value, checked.scope, checked.policy))) throw Error('Invalid durable accepted value');
        await this.validateReplay(checked.events);
        this.state = copy(state);
      }
      // A crash may occur after durable commit but before app materialization.
      // Reconcile before any promise, vote, readiness acknowledgement or read.
      if (!importing && !(this.legacyTrust && !this.state.history.base)) await this.callbacks.materialize(copy(this.checked().events));
      return await operation();
    });
    queues.set(this.guildId, result);
    void result.finally(() => { if (queues!.get(this.guildId) === result) queues!.delete(this.guildId); }).catch(() => undefined);
    return await result;
  }
  private verifyHistory(history: ConsensusHistory) {
    // Reload durable state on every operation; cache only byte-equivalent
    // history, never an untrusted caller's object identity or ballot fields.
    const fingerprint = JSON.stringify(history);
    if (this.verifiedHistory?.fingerprint === fingerprint) return this.verifiedHistory.value;
    const value = verifyConsensusHistory(copy(history), this.guildId, this.anchor, this.legacyTrust);
    this.verifiedHistory = { fingerprint, value };
    return value;
  }
  private checked() { return this.verifyHistory(this.state!.history); }
  private async persist(next: ConsensusPersistentState) {
    await this.store.putConsensusState(this.guildId, copy(next));
    this.state = copy(next);
  }
  private async validateReplay(events: GuildEvent[]) {
    const fingerprint = JSON.stringify(events);
    if (this.replayFingerprint === fingerprint) return;
    for (let index = 0; index < events.length; index++) {
      if (!await this.callbacks.validateEvent(copy(events[index]), copy(events.slice(0, index)), 'replay')) throw Error('Invalid historical application semantics');
    }
    this.replayFingerprint = fingerprint;
  }
  private checkRequest(request: ConsensusPrepareRequest | ConsensusAcceptRequest) {
    const checked = this.checked(), scope = checked.scope;
    if (this.legacyTrust && !this.state!.history.base) throw Error('Verified legacy migration bridge required before v2 voting');
    if (checked.pendingTransition) throw Error('Old epoch retired; joint activation required');
    if (!checked.policy.members.includes(this.identity.publicKey)) throw Error('Local relay is not an active voter');
    if (request.guildId !== scope.guildId || request.index !== scope.index || request.parentHash !== scope.parentHash || request.policyHash !== scope.policyHash || !validConsensusBallot(request.ballot, checked.policy) || !verify(request.ballot.proposer, hashObject(consensusUnsigned(request)), request.signature)) throw Error('Invalid or stale authenticated consensus request');
    if (this.state!.promised && compareConsensusBallots(request.ballot, this.state!.promised) < 0) throw Error('Consensus ballot below durable promise');
    return checked;
  }
  async history() { return this.run(async () => copy(this.state!.history)); }
  async scope() { return this.run(async () => copy(this.checked().scope)); }
  async status() { return this.run(async () => ({ ...copy(this.checked().scope), policy: copy(this.checked().policy), promised: copy(this.state!.promised), pendingTransition: this.checked().pendingTransition ? consensusCommitHash(this.checked().pendingTransition!) : null })); }
  async nextPrepareRequest(minimumCounter = 0): Promise<ConsensusPrepareRequest> {
    return this.run(async () => {
      const checked = this.checked();
      if (this.legacyTrust && !this.state!.history.base) throw Error('Verified legacy migration bridge required before v2 proposing');
      if (checked.pendingTransition || !checked.policy.members.includes(this.identity.publicKey)) throw Error('Local relay cannot propose in this epoch');
      if (!Number.isSafeInteger(minimumCounter) || minimumCounter < 0) throw Error('Invalid ballot floor');
      const counter = Math.max(minimumCounter, this.state!.proposerCounter ?? 0, this.state!.promised?.counter ?? 0) + 1;
      if (!Number.isSafeInteger(counter)) throw Error('Consensus ballot counter exhausted');
      await this.persist({ ...this.state!, proposerCounter: counter });
      const unsigned = { protocol: 'cgp/consensus-prepare/2' as const, ...checked.scope, ballot: { counter, proposer: this.identity.publicKey } };
      return { ...unsigned, signature: await sign(this.identity.privateKey, hashObject(unsigned)) };
    });
  }
  async propose(value: ConsensusValue, transport: ConsensusTransport, minimumCounter = 0) {
    const request = await this.nextPrepareRequest(minimumCounter);
    const policy = (await this.status()).policy;
    const call = (peer: string, method: Parameters<ConsensusTransport['call']>[1], payload: unknown) =>
      peer === this.identity.publicKey ? (this[method] as (value: any) => Promise<unknown>).call(this, payload) : transport.call(peer, method, payload);
    const prepared = await Promise.allSettled(policy.members.map(peer => call(peer, 'prepare', request)));
    const promises = prepared.filter((result): result is PromiseFulfilledResult<unknown> => result.status === 'fulfilled').map(result => result.value as ConsensusPromise);
    const selected = selectConsensusValue(request, request.ballot, policy, promises, value);
    const unsigned = { protocol: 'cgp/consensus-accept/2' as const, guildId: request.guildId, index: request.index, parentHash: request.parentHash,
      policyHash: request.policyHash, ballot: request.ballot, value: selected, promises };
    const accept = { ...unsigned, signature: await sign(this.identity.privateKey, hashObject(unsigned)) };
    const accepted = await Promise.allSettled(policy.members.map(peer => call(peer, 'accept', accept)));
    const votes = accepted.filter((result): result is PromiseFulfilledResult<unknown> => result.status === 'fulfilled').map(result => result.value as ConsensusVote);
    const commit: ConsensusCommit = { protocol: 'cgp/consensus-commit/2', guildId: request.guildId, index: request.index,
      parentHash: request.parentHash, policyHash: request.policyHash, ballot: request.ballot, value: selected, votes };
    if (!verifyConsensusCommit(commit, request, policy)) throw Error('Accept quorum unavailable');
    await this.commit(commit);
    await Promise.allSettled(policy.members.filter(peer => peer !== this.identity.publicKey).map(peer => transport.call(peer, 'commit', commit)));
    return { commit, recoveredDifferentValue: hashObject(selected) !== hashObject(value), history: await this.history() };
  }
  async activateTransition(transport: ConsensusTransport): Promise<ConsensusActivation> {
    const history = await this.history(), checked = this.verifyHistory(history);
    const transition = checked.pendingTransition;
    if (!transition || transition.value.kind !== 'transition') throw Error('No certified transition to activate');
    const next = normalizeConsensusPolicy(transition.value.nextPolicy);
    const readiness = await Promise.allSettled(next.members.map(async peer => {
      if (peer === this.identity.publicKey) return await this.ready();
      await transport.call(peer, 'sync', history);
      return await transport.call(peer, 'ready', null) as ConsensusReady;
    }));
    const activation: ConsensusActivation = { protocol: 'cgp/consensus-activation/2', transition: copy(transition),
      ready: readiness.filter((result): result is PromiseFulfilledResult<ConsensusReady> => result.status === 'fulfilled').map(result => result.value) };
    if (!verifyConsensusActivation(activation, transition)) throw Error('New-policy readiness quorum unavailable');
    await this.activate(activation);
    await Promise.allSettled([...new Set([...checked.policy.members, ...next.members])].filter(peer => peer !== this.identity.publicKey).map(peer => transport.call(peer, 'activate', activation)));
    return activation;
  }
  async prepare(request: ConsensusPrepareRequest): Promise<ConsensusPromise> {
    return this.run(async () => {
      if (request.protocol !== 'cgp/consensus-prepare/2') throw Error('Wrong prepare protocol');
      this.checkRequest(request);
      await this.persist({ ...this.state!, promised: request.ballot });
      const unsigned = { protocol: 'cgp/consensus-promise/2' as const, ...this.checked().scope,
        ballot: request.ballot, relayPublicKey: this.identity.publicKey, accepted: this.state!.accepted };
      return { ...copy(unsigned), signature: await sign(this.identity.privateKey, hashObject(unsigned)) };
    });
  }
  async accept(request: ConsensusAcceptRequest): Promise<ConsensusVote> {
    return this.run(async () => {
      if (request.protocol !== 'cgp/consensus-accept/2') throw Error('Wrong accept protocol');
      const checked = this.checkRequest(request);
      const selected = selectConsensusValue(checked.scope, request.ballot, checked.policy, request.promises, request.value);
      if (hashObject(selected) !== hashObject(request.value)) throw Error('Proposal did not preserve highest accepted value');
      const existing = this.state!.accepted;
      if (existing && compareConsensusBallots(existing.ballot, request.ballot) === 0 && hashObject(existing.value) !== hashObject(request.value)) throw Error('Competing value for the same ballot');
      if (request.value.kind === 'event') {
        const event = request.value.event, previous = checked.events.at(-1);
        if (event.seq !== checked.events.length || event.prevHash !== (previous?.id ?? null) || (!previous && event.body.type !== 'GUILD_CREATE') || !await this.callbacks.validateEvent(copy(event), copy(checked.events), 'accept')) throw Error('Invalid application proposal');
      } else if (checked.epochs.has(request.value.nextPolicy.epoch)) throw Error('Consensus epoch reuse');
      await this.persist({ ...this.state!, promised: request.ballot, accepted: { ballot: request.ballot, value: request.value } });
      const unsigned = { protocol: 'cgp/consensus-vote/2' as const, ...checked.scope,
        ballot: request.ballot, relayPublicKey: this.identity.publicKey, valueHash: hashObject(request.value) };
      return { ...unsigned, signature: await sign(this.identity.privateKey, hashObject(unsigned)) };
    });
  }
  async commit(commit: ConsensusCommit) {
    return this.run(async () => {
      const checked = this.checked();
      if (commit.index < checked.scope.index) {
        const existing = this.state!.history.entries[commit.index]?.commit;
        if (existing && consensusCommitHash(existing) === consensusCommitHash(commit)) return;
        throw Error('Conflicting committed prefix');
      }
      if (checked.pendingTransition || !verifyConsensusCommit(commit, checked.scope, checked.policy)) throw Error('Invalid commit certificate');
      const history = copy(this.state!.history); history.entries.push({ commit: copy(commit) });
      const next = this.verifyHistory(history);
      await this.validateReplay(next.events);
      await this.persist({ ...this.state!, history, promised: null, accepted: null });
      await this.callbacks.materialize(copy(next.events));
    });
  }
  async sync(history: ConsensusHistory) {
    return this.run(async () => {
      const next = this.verifyHistory(history), existing = this.state!.history;
      if (existing.base && (!history.base || consensusMigrationHash(existing.base) !== consensusMigrationHash(history.base))) throw Error('Legacy migration anchor changed');
      if (!existing.base && history.base && (existing.entries.length || this.state!.accepted || this.state!.promised)) throw Error('Cannot replace active v2 history with legacy migration');
      if (history.entries.length < existing.entries.length) throw Error('Consensus history rollback');
      for (let index = 0; index < existing.entries.length; index++) {
        if (consensusCommitHash(existing.entries[index].commit) !== consensusCommitHash(history.entries[index].commit)) throw Error('Conflicting committed prefix');
        if (existing.entries[index].activation && !history.entries[index].activation) throw Error('Activation rollback');
      }
      await this.validateReplay(next.events);
      const sameSlot = hashObject(next.scope) === hashObject(this.checked().scope);
      await this.persist({ ...this.state!, history: copy(history), promised: sameSlot ? this.state!.promised : null, accepted: sameSlot ? this.state!.accepted : null });
      await this.callbacks.materialize(copy(next.events));
    }, true);
  }
  async ready(): Promise<ConsensusReady> {
    return this.run(async () => {
      const transition = this.checked().pendingTransition;
      if (!transition || transition.value.kind !== 'transition') throw Error('No certified transition awaiting activation');
      const policy = normalizeConsensusPolicy(transition.value.nextPolicy);
      if (!policy.members.includes(this.identity.publicKey)) throw Error('Local relay is not a new-epoch voter');
      // Persist again before signing readiness, including the complete old prefix.
      await this.persist(this.state!);
      const unsigned = { protocol: 'cgp/consensus-ready/2' as const, guildId: this.guildId,
        transitionHash: consensusCommitHash(transition), nextPolicyHash: consensusPolicyHash(policy), relayPublicKey: this.identity.publicKey };
      return { ...unsigned, signature: await sign(this.identity.privateKey, hashObject(unsigned)) };
    });
  }
  async activate(activation: ConsensusActivation) {
    return this.run(async () => {
      const transition = this.checked().pendingTransition;
      if (!transition || !verifyConsensusActivation(activation, transition)) throw Error('Joint old/new activation certificate required');
      const history = copy(this.state!.history); history.entries.at(-1)!.activation = copy(activation);
      this.verifyHistory(history);
      await this.persist({ ...this.state!, history, promised: null, accepted: null });
    });
  }
}
