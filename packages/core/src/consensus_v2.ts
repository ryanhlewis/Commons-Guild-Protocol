import { hashObject, verify, verifyObject } from './crypto.js';
import { computeEventId } from './log.js';
import { verifyDeviceAuthorizedObject } from './device_authority.js';
import type { GuildEvent, RelayWriteProposal } from './types.js';
import { verifyRelayWriteCertificate, relayWriteProposalId } from './write_certificate.js';

export interface ConsensusPolicy {
  epoch: string;
  members: string[];
  requiredVotes: number;
  administrators: string[];
  requiredAdministrators: number;
}
export interface ConsensusBallot { counter: number; proposer: string }
export interface ConsensusScope { guildId: string; index: number; parentHash: string | null; policyHash: string }
export interface ConsensusTransition {
  kind: 'transition';
  guildId: string;
  fromPolicyHash: string;
  parentHash: string | null;
  nextPolicy: ConsensusPolicy;
  nonce: string;
  signatures: Array<{ publicKey: string; signature: string }>;
}
export type ConsensusValue = { kind: 'event'; event: GuildEvent } | ConsensusTransition;
export interface ConsensusAccepted { ballot: ConsensusBallot; value: ConsensusValue }
export interface ConsensusPrepareRequest extends ConsensusScope {
  protocol: 'cgp/consensus-prepare/2'; ballot: ConsensusBallot; signature: string;
}
export interface ConsensusAcceptRequest extends ConsensusScope {
  protocol: 'cgp/consensus-accept/2'; ballot: ConsensusBallot; value: ConsensusValue;
  promises: ConsensusPromise[]; signature: string;
}
export interface ConsensusPromise extends ConsensusScope {
  protocol: 'cgp/consensus-promise/2';
  ballot: ConsensusBallot;
  relayPublicKey: string;
  accepted: ConsensusAccepted | null;
  signature: string;
}
export interface ConsensusVote extends ConsensusScope {
  protocol: 'cgp/consensus-vote/2';
  ballot: ConsensusBallot;
  relayPublicKey: string;
  valueHash: string;
  signature: string;
}
export interface ConsensusCommit extends ConsensusScope {
  protocol: 'cgp/consensus-commit/2';
  ballot: ConsensusBallot;
  value: ConsensusValue;
  votes: ConsensusVote[];
}
export interface ConsensusReady {
  protocol: 'cgp/consensus-ready/2';
  guildId: string;
  transitionHash: string;
  nextPolicyHash: string;
  relayPublicKey: string;
  signature: string;
}
export interface ConsensusActivation {
  protocol: 'cgp/consensus-activation/2';
  transition: ConsensusCommit;
  ready: ConsensusReady[];
}
export interface ConsensusHistory {
  protocol: 'cgp/consensus-history/2'; guildId: string; anchorPolicy: ConsensusPolicy;
  base?: LegacyConsensusBridge;
  entries: Array<{ commit: ConsensusCommit; activation?: ConsensusActivation }>;
}
/** Stable across equivalent quorum subsets and later ballots for the same value. */
export function consensusCommitHash(commit: ConsensusCommit) {
  return hashObject({ protocol: 'cgp/consensus-entry/2', guildId: commit.guildId, index: commit.index,
    parentHash: commit.parentHash, policyHash: commit.policyHash, value: commit.value });
}
export function consensusUnsigned<T extends { signature: string }>(value: T): Omit<T, 'signature'> {
  const { signature: _, ...unsigned } = value;
  return unsigned;
}
function canonicalKeys(input: unknown): string[] {
  if (!Array.isArray(input) || input.length === 0 || input.some(key => typeof key !== 'string' || !/^(02|03)[a-f0-9]{64}$/.test(key))) throw Error('Canonical compressed public keys required');
  if (new Set(input).size !== input.length) throw Error('Duplicate consensus identities');
  // Curve validity is checked by signature verification; reject aliases at configuration time.
  return [...input].sort();
}
export function normalizeConsensusPolicy(value: ConsensusPolicy): ConsensusPolicy {
  if (!value || typeof value.epoch !== 'string' || !value.epoch.trim() || value.epoch.length > 200) throw Error('Invalid consensus epoch');
  const members = canonicalKeys(value.members), administrators = canonicalKeys(value.administrators);
  if (members.length < 2 || !Number.isSafeInteger(value.requiredVotes) || value.requiredVotes <= members.length / 2 || value.requiredVotes > members.length) throw Error('Consensus requires an explicit strict majority');
  if (!Number.isSafeInteger(value.requiredAdministrators) || value.requiredAdministrators < 1 || value.requiredAdministrators > administrators.length) throw Error('Invalid administrative threshold');
  return { epoch: value.epoch, members, requiredVotes: value.requiredVotes, administrators, requiredAdministrators: value.requiredAdministrators };
}
export function consensusPolicyHash(policy: ConsensusPolicy) { return hashObject(normalizeConsensusPolicy(policy)); }
export function compareConsensusBallots(a: ConsensusBallot, b: ConsensusBallot) {
  return a.counter === b.counter ? (a.proposer < b.proposer ? -1 : a.proposer > b.proposer ? 1 : 0) : a.counter - b.counter;
}
export function validConsensusBallot(ballot: ConsensusBallot, policy: ConsensusPolicy) {
  return ballot && Number.isSafeInteger(ballot.counter) && ballot.counter >= 0 && policy.members.includes(ballot.proposer);
}
export function consensusTransitionPayload(value: ConsensusTransition) {
  return { kind: value.kind, guildId: value.guildId, fromPolicyHash: value.fromPolicyHash, parentHash: value.parentHash, nextPolicy: normalizeConsensusPolicy(value.nextPolicy), nonce: value.nonce };
}
export function validConsensusValue(value: ConsensusValue, scope: ConsensusScope, policy: ConsensusPolicy) {
  try {
    if (value.kind === 'event') {
      const event = value.event;
      if (event.body.guildId !== scope.guildId || event.id !== computeEventId(event)) return false;
      const unsigned = { body: event.body, author: event.author, createdAt: event.createdAt };
      return event.deviceAuthorization
        ? verifyDeviceAuthorizedObject(unsigned, event.signature, event.deviceAuthorization, { accountPublicKey: event.author, requiredCapability: 'publish', now: event.createdAt }).ok
        : verifyObject(event.author, unsigned, event.signature);
    }
    if (value.kind !== 'transition' || value.guildId !== scope.guildId || value.fromPolicyHash !== scope.policyHash || value.parentHash !== scope.parentHash || typeof value.nonce !== 'string' || !value.nonce || value.nonce.length > 200 || value.nextPolicy.epoch === policy.epoch) return false;
    const signed = consensusTransitionPayload(value), seen = new Set<string>();
    if (!Array.isArray(value.signatures)) return false;
    for (const signature of value.signatures) {
      if (!policy.administrators.includes(signature.publicKey) || seen.has(signature.publicKey) || !verifyObject(signature.publicKey, signed, signature.signature)) return false;
      seen.add(signature.publicKey);
    }
    return seen.size >= policy.requiredAdministrators;
  } catch { return false; }
}
function sameScope(a: ConsensusScope, b: ConsensusScope) {
  return a.guildId === b.guildId && a.index === b.index && a.parentHash === b.parentHash && a.policyHash === b.policyHash;
}
/** A proposer must carry the highest accepted value revealed by a prepare quorum. */
export function selectConsensusValue(scope: ConsensusScope, ballot: ConsensusBallot, policyInput: ConsensusPolicy, promises: ConsensusPromise[], proposed: ConsensusValue): ConsensusValue {
  const policy = normalizeConsensusPolicy(policyInput), seen = new Set<string>();
  if (!validConsensusBallot(ballot, policy) || scope.policyHash !== consensusPolicyHash(policy) || !Array.isArray(promises)) throw Error('Invalid prepare scope');
  let highest: ConsensusAccepted | null = null;
  for (const promise of promises) {
    if (promise.protocol !== 'cgp/consensus-promise/2' || !sameScope(scope, promise) || compareConsensusBallots(promise.ballot, ballot) !== 0 || !policy.members.includes(promise.relayPublicKey) || seen.has(promise.relayPublicKey) || !verify(promise.relayPublicKey, hashObject(consensusUnsigned(promise)), promise.signature)) throw Error('Invalid prepare certificate');
    seen.add(promise.relayPublicKey);
    const accepted = promise.accepted;
    if (!accepted) continue;
    if (!validConsensusBallot(accepted.ballot, policy) || compareConsensusBallots(accepted.ballot, ballot) > 0 || !validConsensusValue(accepted.value, scope, policy)) throw Error('Invalid accepted value');
    const ordering = highest ? compareConsensusBallots(accepted.ballot, highest.ballot) : 1;
    if (ordering === 0 && hashObject(accepted.value) !== hashObject(highest!.value)) throw Error('Equivocating accepted ballot');
    if (ordering > 0) highest = accepted;
  }
  if (seen.size < policy.requiredVotes) throw Error('Prepare quorum unavailable');
  const selected = highest?.value ?? proposed;
  if (!validConsensusValue(selected, scope, policy)) throw Error('Invalid proposed value');
  return selected;
}
export function verifyConsensusCommit(commit: ConsensusCommit, expectedScope: ConsensusScope, policyInput: ConsensusPolicy) {
  try {
    const policy = normalizeConsensusPolicy(policyInput), seen = new Set<string>();
    if (commit.protocol !== 'cgp/consensus-commit/2' || !sameScope(commit, expectedScope) || commit.policyHash !== consensusPolicyHash(policy) || !Number.isSafeInteger(commit.index) || commit.index < 0 || !validConsensusBallot(commit.ballot, policy) || !validConsensusValue(commit.value, commit, policy) || !Array.isArray(commit.votes)) return false;
    for (const vote of commit.votes) {
      if (vote.protocol !== 'cgp/consensus-vote/2' || !sameScope(commit, vote) || compareConsensusBallots(vote.ballot, commit.ballot) !== 0 || vote.valueHash !== hashObject(commit.value) || !policy.members.includes(vote.relayPublicKey) || seen.has(vote.relayPublicKey) || !verify(vote.relayPublicKey, hashObject(consensusUnsigned(vote)), vote.signature)) return false;
      seen.add(vote.relayPublicKey);
    }
    return seen.size >= policy.requiredVotes;
  } catch { return false; }
}
/** The caller must already verify expectedTransition under the pinned old policy. */
export function verifyConsensusActivation(activation: ConsensusActivation, expectedTransition: ConsensusCommit) {
  try {
    if (activation.protocol !== 'cgp/consensus-activation/2' || consensusCommitHash(activation.transition) !== consensusCommitHash(expectedTransition) || expectedTransition.value.kind !== 'transition' || !Array.isArray(activation.ready)) return false;
    const policy = normalizeConsensusPolicy(expectedTransition.value.nextPolicy), seen = new Set<string>();
    for (const ready of activation.ready) {
      if (ready.protocol !== 'cgp/consensus-ready/2' || ready.guildId !== expectedTransition.guildId || ready.transitionHash !== consensusCommitHash(expectedTransition) || ready.nextPolicyHash !== consensusPolicyHash(policy) || !policy.members.includes(ready.relayPublicKey) || seen.has(ready.relayPublicKey) || !verify(ready.relayPublicKey, hashObject(consensusUnsigned(ready)), ready.signature)) return false;
      seen.add(ready.relayPublicKey);
    }
    return seen.size >= policy.requiredVotes;
  } catch { return false; }
}

/** Validate both consensus positions and the independently numbered app log. */
export function verifyConsensusHistory(history: ConsensusHistory, guildId: string, anchor: ConsensusPolicy, legacyTrust?: LegacyConsensusTrust) {
  if (history.protocol !== 'cgp/consensus-history/2' || history.guildId !== guildId || consensusPolicyHash(history.anchorPolicy) !== consensusPolicyHash(anchor) || !Array.isArray(history.entries)) throw Error('Untrusted consensus history anchor');
  let policy = normalizeConsensusPolicy(anchor), parentHash: string | null = history.base ? consensusMigrationHash(history.base) : null;
  let pendingTransition: ConsensusCommit | undefined;
  const events: GuildEvent[] = history.base ? verifyLegacyConsensusBridge(history.base, guildId, anchor, legacyTrust) : [];
  const epochs = new Set([policy.epoch, ...(legacyTrust && history.base ? [legacyTrust.policy.epoch] : [])]);
  for (let index = 0; index < history.entries.length; index++) {
    const { commit, activation } = history.entries[index];
    if (pendingTransition) throw Error('Missing joint activation before next consensus entry');
    const scope = { guildId, index, parentHash, policyHash: consensusPolicyHash(policy) };
    if (!verifyConsensusCommit(commit, scope, policy)) throw Error('Invalid consensus history certificate');
    if (commit.value.kind === 'event') {
      const event = commit.value.event, previous = events.at(-1);
      if (event.seq !== events.length || event.prevHash !== (previous?.id ?? null) || (!previous && event.body.type !== 'GUILD_CREATE')) throw Error('Consensus/app log prefix mismatch');
      events.push(event);
      if (activation) throw Error('Unexpected activation on application event');
    } else {
      if (epochs.has(commit.value.nextPolicy.epoch)) throw Error('Consensus epoch reuse');
      pendingTransition = commit;
      if (activation) {
        if (!verifyConsensusActivation(activation, commit)) throw Error('Invalid joint activation');
        policy = normalizeConsensusPolicy(commit.value.nextPolicy); epochs.add(policy.epoch);
        pendingTransition = undefined;
      }
    }
    parentHash = consensusCommitHash(commit);
  }
  return { policy, pendingTransition, events, epochs,
    scope: { guildId, index: history.entries.length, parentHash, policyHash: consensusPolicyHash(policy) } };
}

export interface LegacyConsensusTrust {
  policy: { epoch: string; members: string[]; requiredVotes: number };
  administrators: string[];
  requiredAdministrators: number;
}
export interface LegacyConsensusMigrationRequest {
  protocol: 'cgp/legacy-migration-request/2'; guildId: string;
  legacyPolicyHash: string; nextPolicyHash: string; nonce: string;
  signatures: Array<{ publicKey: string; signature: string }>;
}
export interface LegacyConsensusFreeze {
  protocol: 'cgp/legacy-freeze/2'; guildId: string; requestHash: string;
  headSeq: number; headHash: string | null; fenceProposalId: string | null;
  relayPublicKey: string; signature: string;
}
export interface LegacyConsensusFreezeRecord {
  request: LegacyConsensusMigrationRequest;
  freeze: LegacyConsensusFreeze;
}
export interface LegacyConsensusBridge {
  protocol: 'cgp/legacy-bridge/2'; request: LegacyConsensusMigrationRequest;
  freezes: LegacyConsensusFreeze[]; events: GuildEvent[]; pendingProposal?: RelayWriteProposal;
}
export function legacyConsensusPolicyHash(trust: LegacyConsensusTrust) {
  const policy = normalizeConsensusPolicy({ ...trust.policy, administrators: trust.administrators, requiredAdministrators: trust.requiredAdministrators });
  return hashObject({ epoch: policy.epoch, members: policy.members, requiredVotes: policy.requiredVotes });
}
export function legacyConsensusRequestPayload(request: LegacyConsensusMigrationRequest) {
  return { protocol: request.protocol, guildId: request.guildId, legacyPolicyHash: request.legacyPolicyHash, nextPolicyHash: request.nextPolicyHash, nonce: request.nonce };
}
export function verifyLegacyConsensusRequest(request: LegacyConsensusMigrationRequest, guildId: string, anchor: ConsensusPolicy, trust: LegacyConsensusTrust) {
  try {
    const administrators = canonicalKeys(trust.administrators), seen = new Set<string>();
    if (request.protocol !== 'cgp/legacy-migration-request/2' || request.guildId !== guildId || request.legacyPolicyHash !== legacyConsensusPolicyHash(trust) || request.nextPolicyHash !== consensusPolicyHash(anchor) || anchor.epoch === trust.policy.epoch || typeof request.nonce !== 'string' || !request.nonce || request.nonce.length > 200 || !Array.isArray(request.signatures)) return false;
    for (const signature of request.signatures) {
      if (!administrators.includes(signature.publicKey) || seen.has(signature.publicKey) || !verifyObject(signature.publicKey, legacyConsensusRequestPayload(request), signature.signature)) return false;
      seen.add(signature.publicKey);
    }
    return seen.size >= trust.requiredAdministrators;
  } catch { return false; }
}
export function verifyLegacyCertifiedPrefix(events: GuildEvent[], guildId: string, trust: LegacyConsensusTrust) {
  if (!Array.isArray(events)) throw Error('Invalid legacy event prefix');
  let previous: GuildEvent | undefined;
  for (let index = 0; index < events.length; index++) {
    const event = events[index];
    if (event.body.guildId !== guildId || event.seq !== index || event.prevHash !== (previous?.id ?? null) || (!previous && event.body.type !== 'GUILD_CREATE') || !verifyRelayWriteCertificate(event) || !event.writeCertificate || legacyConsensusPolicyHash({ ...trust, policy: event.writeCertificate.policy }) !== legacyConsensusPolicyHash(trust)) throw Error('Legacy prefix lacks exact pinned-policy certificates');
    previous = event;
  }
}
export function legacyConsensusPendingEvent(proposal: RelayWriteProposal): GuildEvent {
  const event = { seq: proposal.headSeq + 1, prevHash: proposal.headHash, body: proposal.body as GuildEvent['body'], author: proposal.author, signature: proposal.signature, createdAt: proposal.createdAt,
    ...(proposal.deviceAuthorization ? { deviceAuthorization: proposal.deviceAuthorization } : {}) };
  return { ...event, id: computeEventId(event) };
}
export function consensusMigrationHash(base: LegacyConsensusBridge) {
  const last = base.pendingProposal ? legacyConsensusPendingEvent(base.pendingProposal) : base.events.at(-1);
  return hashObject({ protocol: 'cgp/legacy-migration-anchor/2', request: legacyConsensusRequestPayload(base.request), headSeq: last?.seq ?? -1, headHash: last?.id ?? null });
}
export function verifyLegacyConsensusBridge(base: LegacyConsensusBridge, guildId: string, anchor: ConsensusPolicy, trust?: LegacyConsensusTrust): GuildEvent[] {
  if (!trust || base.protocol !== 'cgp/legacy-bridge/2' || !verifyLegacyConsensusRequest(base.request, guildId, anchor, trust)) throw Error('Explicit trusted legacy policy and authorized migration required');
  verifyLegacyCertifiedPrefix(base.events, guildId, trust);
  const last = base.events.at(-1), seen = new Set<string>(), counts = new Map<string, number>();
  const requestHash = hashObject(legacyConsensusRequestPayload(base.request));
  if (!Array.isArray(base.freezes)) throw Error('Missing legacy freeze inventory');
  for (const freeze of base.freezes) {
    if (freeze.protocol !== 'cgp/legacy-freeze/2' || freeze.guildId !== guildId || freeze.requestHash !== requestHash || freeze.headSeq !== (last?.seq ?? -1) || freeze.headHash !== (last?.id ?? null) || !trust.policy.members.includes(freeze.relayPublicKey) || seen.has(freeze.relayPublicKey) || (freeze.fenceProposalId !== null && !/^[a-f0-9]{64}$/.test(freeze.fenceProposalId)) || !verify(freeze.relayPublicKey, hashObject(consensusUnsigned(freeze)), freeze.signature)) throw Error('Invalid or divergent legacy freeze inventory');
    seen.add(freeze.relayPublicKey);
    if (freeze.fenceProposalId) counts.set(freeze.fenceProposalId, (counts.get(freeze.fenceProposalId) ?? 0) + 1);
  }
  if (seen.size !== trust.policy.members.length) throw Error('Every old voter must durably freeze before legacy migration');
  const possiblyChosen = [...counts].filter(([, count]) => count >= trust.policy.requiredVotes).map(([id]) => id);
  if (possiblyChosen.length > 1) throw Error('Conflicting legacy majority evidence');
  if (!possiblyChosen.length) {
    if (base.pendingProposal) throw Error('Uncertified legacy proposal cannot be imported');
    return [...base.events];
  }
  const proposal = base.pendingProposal;
  if (!proposal || proposal.guildId !== guildId || proposal.headSeq !== (last?.seq ?? -1) || proposal.headHash !== (last?.id ?? null) || relayWriteProposalId(trust.policy.epoch, proposal) !== possiblyChosen[0]) throw Error('Exact potentially chosen legacy proposal is required');
  const event = legacyConsensusPendingEvent(proposal);
  if (!validConsensusValue({ kind: 'event', event }, { guildId, index: 0, parentHash: null, policyHash: consensusPolicyHash(anchor) }, anchor) || (!last && event.body.type !== 'GUILD_CREATE')) throw Error('Invalid potentially chosen legacy event');
  // This is certified by the migration inventory, never a fabricated v1 certificate.
  return [...base.events, event];
}
