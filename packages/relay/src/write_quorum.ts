import {
  hashObject,
  relayWriteProposalId,
  sign,
  verify,
  type RelayWriteProposal,
  type RelayWriteQuorumVote,
  type RelayWriteQuorumVoteUnsigned,
} from "@cgp/core";

export type {
  RelayWriteProposal,
  RelayWriteQuorumVote,
  RelayWriteQuorumVoteUnsigned,
} from "@cgp/core";
export { relayWriteProposalId } from "@cgp/core";

export interface RelayWriteQuorumConfig {
  epoch: string;
  members: string[];
  requiredVotes?: number;
  voteTimeoutMs?: number;
}

export interface RelayWriteVoteFenceStore {
  getWriteVoteFence(key: string): Promise<string | undefined> | string | undefined;
  putWriteVoteFence(key: string, proposalId: string): Promise<void> | void;
}

export interface RelayWriteVoteTransport {
  publish(vote: RelayWriteQuorumVote): Promise<void> | void;
  publishProposal?(proposal: RelayWriteProposal): Promise<void> | void;
  subscribe(
    handler: (vote: RelayWriteQuorumVote) => void,
    proposalHandler?: (proposal: RelayWriteProposal) => void,
  ): Promise<() => Promise<void> | void> | (() => Promise<void> | void);
}

interface PendingVoteWaiter {
  resolve: (votes: RelayWriteQuorumVote[]) => void;
  reject: (error: Error) => void;
  timer: NodeJS.Timeout;
}

function uniqueMembers(members: string[]) {
  return Array.from(
    new Set(members.map((member) => member.trim().toLowerCase()).filter(Boolean)),
  );
}

export function normalizeRelayWriteQuorumConfig(
  config: RelayWriteQuorumConfig,
): Required<RelayWriteQuorumConfig> {
  const epoch = config.epoch?.trim();
  const members = uniqueMembers(config.members ?? []);
  if (!epoch) {
    throw new Error("Relay write quorum requires an epoch");
  }
  if (members.length < 2) {
    throw new Error("Relay write quorum requires at least two relay members");
  }
  const majority = Math.floor(members.length / 2) + 1;
  const requiredVotes = Math.max(
    majority,
    Math.min(members.length, Math.floor(config.requiredVotes ?? majority)),
  );
  const voteTimeoutMs = Math.max(
    100,
    Math.min(30_000, Math.floor(config.voteTimeoutMs ?? 2_000)),
  );
  return { epoch, members, requiredVotes, voteTimeoutMs };
}

export function relayWriteQuorumConfigFromEnv(
  raw = process.env.CGP_RELAY_WRITE_QUORUM_CONFIG,
): RelayWriteQuorumConfig | undefined {
  if (!raw?.trim()) {
    return undefined;
  }
  const parsed = JSON.parse(raw) as RelayWriteQuorumConfig;
  return normalizeRelayWriteQuorumConfig(parsed);
}

function voteSigningPayload(
  vote: RelayWriteQuorumVoteUnsigned,
): RelayWriteQuorumVoteUnsigned {
  return {
    protocol: "cgp/write-vote/1",
    epoch: vote.epoch,
    relayPublicKey: vote.relayPublicKey,
    guildId: vote.guildId,
    headSeq: vote.headSeq,
    headHash: vote.headHash,
    proposalId: vote.proposalId,
    votedAt: vote.votedAt,
  };
}

function voteFenceKey(
  epoch: string,
  proposal: Pick<RelayWriteProposal, "guildId" | "headSeq" | "headHash">,
) {
  return hashObject({
    protocol: "cgp/write-fence/1",
    epoch,
    guildId: proposal.guildId,
    headSeq: proposal.headSeq,
    headHash: proposal.headHash,
  });
}

export class RelayWriteQuorumCoordinator {
  readonly config: Required<RelayWriteQuorumConfig>;
  private memberSet: Set<string>;
  private votesByProposal = new Map<string, Map<string, RelayWriteQuorumVote>>();
  private waiters = new Map<string, Set<PendingVoteWaiter>>();
  private unsubscribe?: () => Promise<void> | void;
  private startPromise?: Promise<void>;
  private proposalsInFlight = new Set<string>();
  private fenceMutations = new Map<string, Promise<void>>();
  private closed = false;

  constructor(
    config: RelayWriteQuorumConfig,
    private identity: { publicKey: string; privateKey: Uint8Array },
    private store: RelayWriteVoteFenceStore,
    private transport: RelayWriteVoteTransport,
    private validateProposal: (
      proposal: RelayWriteProposal,
    ) => Promise<boolean> | boolean = () => true,
  ) {
    this.config = normalizeRelayWriteQuorumConfig(config);
    this.memberSet = new Set(this.config.members);
    if (!this.memberSet.has(identity.publicKey.toLowerCase())) {
      throw new Error("Local relay key is not a member of the write quorum epoch");
    }
  }

  start() {
    if (this.startPromise) {
      return this.startPromise;
    }
    this.startPromise = Promise.resolve(
      this.transport.subscribe(
        (vote) => this.observeVote(vote),
        (proposal) => {
          void this.observeProposal(proposal);
        },
      ),
    ).then((unsubscribe) => {
      if (this.closed) {
        return Promise.resolve(unsubscribe()).then(() => undefined);
      }
      this.unsubscribe = unsubscribe;
    });
    return this.startPromise;
  }

  async authorize(proposal: RelayWriteProposal) {
    if (this.closed) {
      throw new Error("Relay write quorum coordinator is closed");
    }
    await this.start();
    const proposalId = relayWriteProposalId(this.config.epoch, proposal);
    await this.castVote(proposal, proposalId);
    await this.transport.publishProposal?.(proposal);
    return await this.waitForQuorum(proposalId);
  }

  private async castVote(proposal: RelayWriteProposal, proposalId: string) {
    const fenceKey = voteFenceKey(this.config.epoch, proposal);
    return await this.withFenceMutation(fenceKey, async () => {
      const existingVote = await this.store.getWriteVoteFence(fenceKey);
      if (existingVote && existingVote !== proposalId) {
        throw new Error("Relay already voted for a competing proposal at this guild head");
      }
      if (!existingVote) {
        await this.store.putWriteVoteFence(fenceKey, proposalId);
      }

      const unsigned: RelayWriteQuorumVoteUnsigned = {
        protocol: "cgp/write-vote/1",
        epoch: this.config.epoch,
        relayPublicKey: this.identity.publicKey.toLowerCase(),
        guildId: proposal.guildId,
        headSeq: proposal.headSeq,
        headHash: proposal.headHash,
        proposalId,
        votedAt: Date.now(),
      };
      const vote: RelayWriteQuorumVote = {
        ...unsigned,
        signature: await sign(
          this.identity.privateKey,
          hashObject(voteSigningPayload(unsigned)),
        ),
      };
      this.observeVote(vote);
      await this.transport.publish(vote);
      return vote;
    });
  }

  private withFenceMutation<T>(key: string, task: () => Promise<T>) {
    const previous = this.fenceMutations.get(key) ?? Promise.resolve();
    const result = previous.catch(() => undefined).then(task);
    const settled = result.then(
      () => undefined,
      () => undefined,
    );
    this.fenceMutations.set(key, settled);
    void settled.finally(() => {
      if (this.fenceMutations.get(key) === settled) {
        this.fenceMutations.delete(key);
      }
    });
    return result;
  }

  private async observeProposal(proposal: RelayWriteProposal) {
    if (this.closed || !proposal || typeof proposal !== "object") {
      return false;
    }
    const proposalId = relayWriteProposalId(this.config.epoch, proposal);
    if (
      this.votesByProposal.get(proposalId)?.has(this.identity.publicKey.toLowerCase()) ||
      this.proposalsInFlight.has(proposalId)
    ) {
      return false;
    }
    this.proposalsInFlight.add(proposalId);
    try {
      if (!(await this.validateProposal(proposal))) return false;
      await this.castVote(proposal, proposalId);
      return true;
    } catch {
      return false;
    } finally {
      this.proposalsInFlight.delete(proposalId);
    }
  }

  observeVote(vote: RelayWriteQuorumVote) {
    if (!this.verifyVote(vote)) {
      return false;
    }
    const relayKey = vote.relayPublicKey.toLowerCase();
    const votes = this.votesByProposal.get(vote.proposalId) ?? new Map();
    votes.set(relayKey, { ...vote, relayPublicKey: relayKey });
    this.votesByProposal.set(vote.proposalId, votes);
    this.resolveWaiters(vote.proposalId);
    this.pruneVotes();
    return true;
  }

  async close() {
    this.closed = true;
    for (const waiters of this.waiters.values()) {
      for (const waiter of waiters) {
        clearTimeout(waiter.timer);
        waiter.reject(new Error("Relay write quorum coordinator closed"));
      }
    }
    this.waiters.clear();
    const unsubscribe = this.unsubscribe;
    this.unsubscribe = undefined;
    await unsubscribe?.();
    this.votesByProposal.clear();
    this.proposalsInFlight.clear();
    this.fenceMutations.clear();
  }

  private verifyVote(vote: RelayWriteQuorumVote) {
    if (
      vote?.protocol !== "cgp/write-vote/1" ||
      vote.epoch !== this.config.epoch ||
      !this.memberSet.has(vote.relayPublicKey?.toLowerCase()) ||
      typeof vote.guildId !== "string" ||
      !Number.isSafeInteger(vote.headSeq) ||
      vote.headSeq < -1 ||
      (vote.headHash !== null && typeof vote.headHash !== "string") ||
      typeof vote.proposalId !== "string" ||
      !vote.proposalId ||
      typeof vote.votedAt !== "number" ||
      Math.abs(Date.now() - vote.votedAt) > 5 * 60 * 1000 ||
      typeof vote.signature !== "string"
    ) {
      return false;
    }
    return verify(
      vote.relayPublicKey,
      hashObject(voteSigningPayload(vote)),
      vote.signature,
    );
  }

  private waitForQuorum(proposalId: string) {
    const current = this.quorumVotes(proposalId);
    if (current) {
      return Promise.resolve(current);
    }
    return new Promise<RelayWriteQuorumVote[]>((resolve, reject) => {
      const waiters = this.waiters.get(proposalId) ?? new Set<PendingVoteWaiter>();
      const waiter: PendingVoteWaiter = {
        resolve,
        reject,
        timer: setTimeout(() => {
          waiters.delete(waiter);
          if (waiters.size === 0) {
            this.waiters.delete(proposalId);
          }
          reject(
            new Error(
              `Relay write quorum unavailable (${this.votesByProposal.get(proposalId)?.size ?? 0}/${this.config.requiredVotes} votes)`,
            ),
          );
        }, this.config.voteTimeoutMs),
      };
      waiter.timer.unref?.();
      waiters.add(waiter);
      this.waiters.set(proposalId, waiters);
      this.resolveWaiters(proposalId);
    });
  }

  private quorumVotes(proposalId: string) {
    const votes = this.votesByProposal.get(proposalId);
    return votes && votes.size >= this.config.requiredVotes
      ? Array.from(votes.values()).slice(0, this.config.requiredVotes)
      : undefined;
  }

  private resolveWaiters(proposalId: string) {
    const votes = this.quorumVotes(proposalId);
    if (!votes) {
      return;
    }
    const waiters = this.waiters.get(proposalId);
    if (!waiters) {
      return;
    }
    this.waiters.delete(proposalId);
    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.resolve(votes);
    }
  }

  private pruneVotes() {
    if (this.votesByProposal.size <= 10_000) {
      return;
    }
    while (this.votesByProposal.size > 8_000) {
      const oldest = this.votesByProposal.keys().next().value;
      if (!oldest) {
        break;
      }
      this.votesByProposal.delete(oldest);
    }
  }
}
