import {
  hashObject,
  sign,
  verify,
  type DeviceAuthorization,
} from "@cgp/core";

export interface RelaySequencerConsensusConfig {
  epoch: string;
  members: string[];
  requiredVotes?: number;
  electionTimeoutMinMs?: number;
  electionTimeoutMaxMs?: number;
  heartbeatIntervalMs?: number;
  requestTimeoutMs?: number;
  /**
   * Reject writes received by a known follower immediately so a client can
   * retry the relay that currently owns the sequencing lease.
   */
  redirectFollowers?: boolean;
}

export interface RelaySequencerHead {
  headSeq: number;
  headHash: string | null;
}

export interface RelaySequencingRequest {
  guildId: string;
  body: unknown;
  author: string;
  signature: string;
  deviceAuthorization?: DeviceAuthorization;
  createdAt: number;
  clientEventId?: string;
}

export interface RelaySequencingToken {
  epoch: string;
  guildId: string;
  term: number;
  leaderPublicKey: string;
  requestId: string;
  headSeq: number;
  headHash: string | null;
}

export interface RelaySequencerPersistentState {
  term: number;
  votedFor?: string;
}

interface SignedElectionRequest {
  protocol: "cgp/sequencer/1";
  kind: "election-request";
  epoch: string;
  guildId: string;
  term: number;
  candidatePublicKey: string;
  headSeq: number;
  headHash: string | null;
  sentAt: number;
  signature: string;
}

export interface SignedElectionVote {
  protocol: "cgp/sequencer/1";
  kind: "election-vote";
  epoch: string;
  guildId: string;
  term: number;
  candidatePublicKey: string;
  candidateHeadSeq: number;
  candidateHeadHash: string | null;
  voterPublicKey: string;
  votedAt: number;
  signature: string;
}

interface SignedLeaderHeartbeat {
  protocol: "cgp/sequencer/1";
  kind: "leader-heartbeat";
  epoch: string;
  guildId: string;
  term: number;
  leaderPublicKey: string;
  certificate: SignedElectionVote[];
  sentAt: number;
  signature: string;
}

interface SignedSequencingProposal {
  protocol: "cgp/sequencer/1";
  kind: "sequencing-proposal";
  epoch: string;
  guildId: string;
  term: number;
  leaderPublicKey: string;
  requestId: string;
  headSeq: number;
  headHash: string | null;
  certificate: SignedElectionVote[];
  selectedAt: number;
  signature: string;
}

export type RelaySequencerMessage =
  | SignedElectionRequest
  | SignedElectionVote
  | SignedLeaderHeartbeat
  | SignedSequencingProposal;

type UnsignedRelaySequencerMessage =
  | Omit<SignedElectionRequest, "signature">
  | Omit<SignedElectionVote, "signature">
  | Omit<SignedLeaderHeartbeat, "signature">
  | Omit<SignedSequencingProposal, "signature">;

export interface RelaySequencerStateStore {
  getSequencerState(
    key: string,
  ):
    | Promise<RelaySequencerPersistentState | undefined>
    | RelaySequencerPersistentState
    | undefined;
  putSequencerState(
    key: string,
    state: RelaySequencerPersistentState,
  ): Promise<void> | void;
}

export interface RelaySequencerTransport {
  publish(message: RelaySequencerMessage): Promise<void> | void;
  subscribe(
    handler: (message: RelaySequencerMessage) => void,
  ): Promise<() => Promise<void> | void> | (() => Promise<void> | void);
}

interface RequestWaiter {
  resolve: (token: RelaySequencingToken) => void;
  reject: (error: Error) => void;
  timer: NodeJS.Timeout;
}

interface GuildRuntime {
  guildId: string;
  stateKey: string;
  state: RelaySequencerPersistentState;
  loaded: Promise<void>;
  mutation: Promise<void>;
  role: "follower" | "candidate" | "leader";
  leaderPublicKey?: string;
  leaderCertificate?: SignedElectionVote[];
  electionTimer?: NodeJS.Timeout;
  heartbeatTimer?: NodeJS.Timeout;
  votes: Map<string, SignedElectionVote>;
  queued: string[];
  queuedIds: Set<string>;
  activeRequestId?: string;
  tokens: Map<string, RelaySequencingToken>;
  waiters: Map<string, Set<RequestWaiter>>;
}

function uniqueMembers(members: string[]) {
  return Array.from(
    new Set(members.map((member) => member.trim().toLowerCase()).filter(Boolean)),
  );
}

export function normalizeRelaySequencerConsensusConfig(
  config: RelaySequencerConsensusConfig,
): Required<RelaySequencerConsensusConfig> {
  const epoch = config.epoch?.trim();
  const members = uniqueMembers(config.members ?? []);
  if (!epoch) {
    throw new Error("Relay sequencer consensus requires an epoch");
  }
  if (members.length < 2) {
    throw new Error("Relay sequencer consensus requires at least two relay members");
  }
  const majority = Math.floor(members.length / 2) + 1;
  const requiredVotes = Math.max(
    majority,
    Math.min(members.length, Math.floor(config.requiredVotes ?? majority)),
  );
  const electionTimeoutMinMs = Math.max(
    100,
    Math.min(30_000, Math.floor(config.electionTimeoutMinMs ?? 450)),
  );
  const electionTimeoutMaxMs = Math.max(
    electionTimeoutMinMs + members.length,
    Math.min(60_000, Math.floor(config.electionTimeoutMaxMs ?? 900)),
  );
  const heartbeatIntervalMs = Math.max(
    25,
    Math.min(
      Math.floor(electionTimeoutMinMs / 3),
      Math.floor(config.heartbeatIntervalMs ?? 100),
    ),
  );
  const requestTimeoutMs = Math.max(
    electionTimeoutMaxMs * 2,
    Math.min(60_000, Math.floor(config.requestTimeoutMs ?? 8_000)),
  );
  const redirectFollowers = config.redirectFollowers === true;
  return {
    epoch,
    members,
    requiredVotes,
    electionTimeoutMinMs,
    electionTimeoutMaxMs,
    heartbeatIntervalMs,
    requestTimeoutMs,
    redirectFollowers,
  };
}

export function relaySequencerConsensusConfigFromEnv(
  raw = process.env.CGP_RELAY_SEQUENCER_CONFIG,
): RelaySequencerConsensusConfig | undefined {
  if (!raw?.trim()) {
    return undefined;
  }
  return normalizeRelaySequencerConsensusConfig(
    JSON.parse(raw) as RelaySequencerConsensusConfig,
  );
}

export function relaySequencingRequestId(
  epoch: string,
  request: RelaySequencingRequest,
) {
  return hashObject({
    protocol: "cgp/sequencing-request/1",
    epoch,
    guildId: request.guildId,
    body: request.body,
    author: request.author,
    signature: request.signature,
    deviceAuthorization: request.deviceAuthorization,
    createdAt: request.createdAt,
    clientEventId: request.clientEventId,
  });
}

function messageSigningPayload(message: RelaySequencerMessage) {
  const { signature: _signature, ...unsigned } = message;
  return unsigned;
}

function isSafeTerm(value: unknown) {
  return Number.isSafeInteger(value) && Number(value) > 0;
}

function isSafeHead(headSeq: unknown, headHash: unknown) {
  return (
    Number.isSafeInteger(headSeq) &&
    Number(headSeq) >= -1 &&
    (headHash === null || typeof headHash === "string")
  );
}

function isFreshTimestamp(value: unknown) {
  return (
    typeof value === "number" &&
    Number.isFinite(value) &&
    Math.abs(Date.now() - value) <= 5 * 60 * 1000
  );
}

function candidateIsCurrent(
  candidate: RelaySequencerHead,
  local: RelaySequencerHead,
) {
  return (
    candidate.headSeq > local.headSeq ||
    (candidate.headSeq === local.headSeq &&
      candidate.headHash === local.headHash)
  );
}

export class RelaySequencerConsensusCoordinator {
  readonly config: Required<RelaySequencerConsensusConfig>;
  private memberSet: Set<string>;
  private guilds = new Map<string, GuildRuntime>();
  private unsubscribe?: () => Promise<void> | void;
  private startPromise?: Promise<void>;
  private closed = false;

  constructor(
    config: RelaySequencerConsensusConfig,
    private identity: { publicKey: string; privateKey: Uint8Array },
    private store: RelaySequencerStateStore,
    private transport: RelaySequencerTransport,
    private readHead: (
      guildId: string,
    ) => Promise<RelaySequencerHead> | RelaySequencerHead,
    private random: () => number = Math.random,
  ) {
    this.config = normalizeRelaySequencerConsensusConfig(config);
    this.memberSet = new Set(this.config.members);
    this.identity = {
      ...identity,
      publicKey: identity.publicKey.toLowerCase(),
    };
    if (!this.memberSet.has(this.identity.publicKey)) {
      throw new Error("Local relay key is not a sequencer consensus member");
    }
  }

  start() {
    if (this.startPromise) {
      return this.startPromise;
    }
    this.startPromise = Promise.resolve(
      this.transport.subscribe((message) => {
        void this.observeMessage(message);
      }),
    ).then((unsubscribe) => {
      if (this.closed) {
        return Promise.resolve(unsubscribe()).then(() => undefined);
      }
      this.unsubscribe = unsubscribe;
    });
    return this.startPromise;
  }

  async sequence(request: RelaySequencingRequest) {
    if (this.closed) {
      throw new Error("Relay sequencer consensus coordinator is closed");
    }
    await this.start();
    const runtime = await this.ensureGuild(request.guildId);
    if (
      this.config.redirectFollowers &&
      runtime.role === "follower" &&
      runtime.leaderPublicKey &&
      runtime.leaderPublicKey !== this.identity.publicKey
    ) {
      throw this.redirectError(runtime);
    }
    const requestId = relaySequencingRequestId(this.config.epoch, request);
    const existing = runtime.tokens.get(requestId);
    if (
      existing &&
      existing.term === runtime.state.term &&
      existing.leaderPublicKey === runtime.leaderPublicKey
    ) {
      return existing;
    }
    if (existing) {
      runtime.tokens.delete(requestId);
    }

    const tokenPromise = this.waitForRequest(runtime, requestId);
    await this.mutate(runtime, async () => {
      if (!runtime.queuedIds.has(requestId)) {
        runtime.queuedIds.add(requestId);
        runtime.queued.push(requestId);
      }
      await this.selectNext(runtime);
      this.armElection(runtime);
    });
    return await tokenPromise;
  }

  async complete(token: RelaySequencingToken, finalized = true) {
    const runtime = this.guilds.get(token.guildId);
    if (!runtime) {
      return;
    }
    await runtime.loaded;
    await this.mutate(runtime, async () => {
      if (
        runtime.role !== "leader" ||
        token.term !== runtime.state.term ||
        token.leaderPublicKey !== this.identity.publicKey ||
        runtime.activeRequestId !== token.requestId
      ) {
        return;
      }
      // A witness vote fence makes this exact proposal the only safe write at
      // the selected head. Keep the slot active after a transient quorum or
      // replication failure so an exact idempotent retry can finish it. A
      // deterministically rejected request is finalized without committing an
      // event, otherwise one invalid client request could fence the guild.
      if (!finalized) {
        return;
      }
      runtime.activeRequestId = undefined;
      await this.selectNext(runtime);
    });
  }

  diagnostics(guildId: string) {
    const runtime = this.guilds.get(guildId);
    if (!runtime) {
      return {
        epoch: this.config.epoch,
        guildId,
        role: "inactive" as const,
        term: 0,
        localPublicKey: this.identity.publicKey,
      };
    }
    return {
      epoch: this.config.epoch,
      guildId,
      role: runtime.role,
      term: runtime.state.term,
      localPublicKey: this.identity.publicKey,
      leaderPublicKey: runtime.leaderPublicKey,
      queuedRequests: runtime.queued.length,
      activeRequestId: runtime.activeRequestId,
      certificateVotes: runtime.leaderCertificate?.length ?? 0,
    };
  }

  async close() {
    this.closed = true;
    for (const runtime of this.guilds.values()) {
      this.clearElectionTimer(runtime);
      this.clearHeartbeatTimer(runtime);
      for (const waiters of runtime.waiters.values()) {
        for (const waiter of waiters) {
          clearTimeout(waiter.timer);
          waiter.reject(new Error("Relay sequencer consensus coordinator closed"));
        }
      }
      runtime.waiters.clear();
    }
    const unsubscribe = this.unsubscribe;
    this.unsubscribe = undefined;
    await unsubscribe?.();
    this.guilds.clear();
  }

  private async ensureGuild(guildId: string) {
    let runtime = this.guilds.get(guildId);
    if (!runtime) {
      const stateKey = hashObject({
        protocol: "cgp/sequencer-state/1",
        epoch: this.config.epoch,
        guildId,
      });
      runtime = {
        guildId,
        stateKey,
        state: { term: 0 },
        loaded: Promise.resolve(),
        mutation: Promise.resolve(),
        role: "follower",
        votes: new Map(),
        queued: [],
        queuedIds: new Set(),
        tokens: new Map(),
        waiters: new Map(),
      };
      runtime.loaded = Promise.resolve(
        this.store.getSequencerState(stateKey),
      ).then((stored) => {
        if (
          stored &&
          Number.isSafeInteger(stored.term) &&
          stored.term >= 0 &&
          (!stored.votedFor ||
            this.memberSet.has(stored.votedFor.toLowerCase()))
        ) {
          runtime!.state = {
            term: stored.term,
            votedFor: stored.votedFor?.toLowerCase(),
          };
        }
      });
      this.guilds.set(guildId, runtime);
    }
    await runtime.loaded;
    return runtime;
  }

  private mutate<T>(runtime: GuildRuntime, task: () => Promise<T>) {
    const result = runtime.mutation.catch(() => undefined).then(task);
    runtime.mutation = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private electionDelayMs() {
    const memberIndex = Math.max(
      0,
      this.config.members.indexOf(this.identity.publicKey),
    );
    const width =
      this.config.electionTimeoutMaxMs - this.config.electionTimeoutMinMs;
    const bucketWidth = Math.max(
      1,
      Math.floor(width / this.config.members.length),
    );
    const bucketStart =
      this.config.electionTimeoutMinMs + memberIndex * bucketWidth;
    const bucketEnd =
      memberIndex === this.config.members.length - 1
        ? this.config.electionTimeoutMaxMs
        : Math.min(
            this.config.electionTimeoutMaxMs,
            bucketStart + bucketWidth - 1,
          );
    return (
      bucketStart +
      Math.floor(this.random() * Math.max(1, bucketEnd - bucketStart + 1))
    );
  }

  private armElection(runtime: GuildRuntime) {
    if (this.closed || runtime.role === "leader") {
      return;
    }
    this.clearElectionTimer(runtime);
    runtime.electionTimer = setTimeout(() => {
      void this.beginElection(runtime);
    }, this.electionDelayMs());
    runtime.electionTimer.unref?.();
  }

  private clearElectionTimer(runtime: GuildRuntime) {
    if (runtime.electionTimer) {
      clearTimeout(runtime.electionTimer);
      runtime.electionTimer = undefined;
    }
  }

  private clearHeartbeatTimer(runtime: GuildRuntime) {
    if (runtime.heartbeatTimer) {
      clearInterval(runtime.heartbeatTimer);
      runtime.heartbeatTimer = undefined;
    }
  }

  private async beginElection(runtime: GuildRuntime) {
    if (this.closed) {
      return;
    }
    await runtime.loaded;
    await this.mutate(runtime, async () => {
      if (runtime.role === "leader") {
        return;
      }
      const head = await this.readHead(runtime.guildId);
      runtime.state = {
        term: runtime.state.term + 1,
        votedFor: this.identity.publicKey,
      };
      await this.store.putSequencerState(runtime.stateKey, runtime.state);
      runtime.role = "candidate";
      runtime.leaderPublicKey = undefined;
      runtime.leaderCertificate = undefined;
      runtime.votes.clear();
      runtime.tokens.clear();
      runtime.activeRequestId = undefined;
      const request = await this.signMessage({
        protocol: "cgp/sequencer/1",
        kind: "election-request",
        epoch: this.config.epoch,
        guildId: runtime.guildId,
        term: runtime.state.term,
        candidatePublicKey: this.identity.publicKey,
        headSeq: head.headSeq,
        headHash: head.headHash,
        sentAt: Date.now(),
      });
      const selfVote = await this.createElectionVote(request);
      runtime.votes.set(this.identity.publicKey, selfVote);
      await this.transport.publish(request);
      await this.maybeBecomeLeader(runtime);
      this.armElection(runtime);
    });
  }

  private async observeMessage(message: RelaySequencerMessage) {
    if (
      this.closed ||
      !message ||
      message.protocol !== "cgp/sequencer/1" ||
      message.epoch !== this.config.epoch ||
      typeof message.guildId !== "string" ||
      !message.guildId
    ) {
      return;
    }
    const runtime = await this.ensureGuild(message.guildId);
    if (message.kind === "election-request") {
      await this.observeElectionRequest(runtime, message);
    } else if (message.kind === "election-vote") {
      await this.observeElectionVote(runtime, message);
    } else if (message.kind === "leader-heartbeat") {
      await this.observeHeartbeat(runtime, message);
    } else if (message.kind === "sequencing-proposal") {
      await this.observeProposal(runtime, message);
    }
  }

  private validElectionRequest(message: SignedElectionRequest) {
    return (
      this.memberSet.has(message.candidatePublicKey?.toLowerCase()) &&
      isSafeTerm(message.term) &&
      isSafeHead(message.headSeq, message.headHash) &&
      isFreshTimestamp(message.sentAt) &&
      verify(
        message.candidatePublicKey,
        hashObject(messageSigningPayload(message)),
        message.signature,
      )
    );
  }

  private validElectionVote(message: SignedElectionVote) {
    return (
      this.memberSet.has(message.candidatePublicKey?.toLowerCase()) &&
      this.memberSet.has(message.voterPublicKey?.toLowerCase()) &&
      isSafeTerm(message.term) &&
      isSafeHead(message.candidateHeadSeq, message.candidateHeadHash) &&
      typeof message.votedAt === "number" &&
      Number.isFinite(message.votedAt) &&
      verify(
        message.voterPublicKey,
        hashObject(messageSigningPayload(message)),
        message.signature,
      )
    );
  }

  private async observeElectionRequest(
    runtime: GuildRuntime,
    raw: SignedElectionRequest,
  ) {
    const message = {
      ...raw,
      candidatePublicKey: raw.candidatePublicKey?.toLowerCase(),
    };
    if (!this.validElectionRequest(message)) {
      return;
    }
    await this.mutate(runtime, async () => {
      if (message.term < runtime.state.term) {
        return;
      }
      if (
        message.term === runtime.state.term &&
        message.candidatePublicKey === this.identity.publicKey &&
        (runtime.role === "candidate" || runtime.role === "leader")
      ) {
        return;
      }
      const localHead = await this.readHead(runtime.guildId);
      if (
        !candidateIsCurrent(
          { headSeq: message.headSeq, headHash: message.headHash },
          localHead,
        )
      ) {
        return;
      }
      if (
        message.term === runtime.state.term &&
        runtime.state.votedFor &&
        runtime.state.votedFor !== message.candidatePublicKey
      ) {
        return;
      }
      if (
        message.term > runtime.state.term ||
        runtime.state.votedFor !== message.candidatePublicKey
      ) {
        runtime.state = {
          term: message.term,
          votedFor: message.candidatePublicKey,
        };
        await this.store.putSequencerState(runtime.stateKey, runtime.state);
      }
      this.becomeFollower(runtime);
      this.armElection(runtime);
      await this.transport.publish(await this.createElectionVote(message));
    });
  }

  private async createElectionVote(message: SignedElectionRequest) {
    return await this.signMessage({
      protocol: "cgp/sequencer/1",
      kind: "election-vote",
      epoch: this.config.epoch,
      guildId: message.guildId,
      term: message.term,
      candidatePublicKey: message.candidatePublicKey,
      candidateHeadSeq: message.headSeq,
      candidateHeadHash: message.headHash,
      voterPublicKey: this.identity.publicKey,
      votedAt: Date.now(),
    });
  }

  private async observeElectionVote(
    runtime: GuildRuntime,
    raw: SignedElectionVote,
  ) {
    const message = {
      ...raw,
      candidatePublicKey: raw.candidatePublicKey?.toLowerCase(),
      voterPublicKey: raw.voterPublicKey?.toLowerCase(),
    };
    if (!this.validElectionVote(message)) {
      return;
    }
    await this.mutate(runtime, async () => {
      if (
        runtime.role !== "candidate" ||
        message.term !== runtime.state.term ||
        message.candidatePublicKey !== this.identity.publicKey
      ) {
        return;
      }
      runtime.votes.set(message.voterPublicKey, message);
      await this.maybeBecomeLeader(runtime);
    });
  }

  private async maybeBecomeLeader(runtime: GuildRuntime) {
    if (
      runtime.role !== "candidate" ||
      runtime.votes.size < this.config.requiredVotes
    ) {
      return;
    }
    const certificate = Array.from(runtime.votes.values()).slice(
      0,
      this.config.requiredVotes,
    );
    runtime.role = "leader";
    runtime.leaderPublicKey = this.identity.publicKey;
    runtime.leaderCertificate = certificate;
    this.clearElectionTimer(runtime);
    await this.broadcastHeartbeat(runtime);
    this.clearHeartbeatTimer(runtime);
    runtime.heartbeatTimer = setInterval(() => {
      void this.mutate(runtime, async () => {
        if (runtime.role === "leader") {
          await this.broadcastHeartbeat(runtime);
        }
      });
    }, this.config.heartbeatIntervalMs);
    runtime.heartbeatTimer.unref?.();
    await this.selectNext(runtime);
  }

  private async broadcastHeartbeat(runtime: GuildRuntime) {
    if (!runtime.leaderCertificate) {
      return;
    }
    await this.transport.publish(
      await this.signMessage({
        protocol: "cgp/sequencer/1",
        kind: "leader-heartbeat",
        epoch: this.config.epoch,
        guildId: runtime.guildId,
        term: runtime.state.term,
        leaderPublicKey: this.identity.publicKey,
        certificate: runtime.leaderCertificate,
        sentAt: Date.now(),
      }),
    );
  }

  private validLeaderCertificate(
    guildId: string,
    term: number,
    leaderPublicKey: string,
    certificate: SignedElectionVote[],
  ) {
    if (!Array.isArray(certificate)) {
      return false;
    }
    const voters = new Set<string>();
    for (const rawVote of certificate) {
      const vote = {
        ...rawVote,
        candidatePublicKey: rawVote.candidatePublicKey?.toLowerCase(),
        voterPublicKey: rawVote.voterPublicKey?.toLowerCase(),
      };
      if (
        vote.guildId !== guildId ||
        vote.term !== term ||
        vote.candidatePublicKey !== leaderPublicKey ||
        !this.validElectionVote(vote)
      ) {
        return false;
      }
      voters.add(vote.voterPublicKey);
    }
    return voters.size >= this.config.requiredVotes;
  }

  private async observeHeartbeat(
    runtime: GuildRuntime,
    raw: SignedLeaderHeartbeat,
  ) {
    const message = {
      ...raw,
      leaderPublicKey: raw.leaderPublicKey?.toLowerCase(),
    };
    if (
      !this.memberSet.has(message.leaderPublicKey) ||
      !isSafeTerm(message.term) ||
      !isFreshTimestamp(message.sentAt) ||
      !this.validLeaderCertificate(
        message.guildId,
        message.term,
        message.leaderPublicKey,
        message.certificate,
      ) ||
      !verify(
        message.leaderPublicKey,
        hashObject(messageSigningPayload(message)),
        message.signature,
      )
    ) {
      return;
    }
    await this.mutate(runtime, async () => {
      if (message.term < runtime.state.term) {
        return;
      }
      await this.acceptLeader(
        runtime,
        message.term,
        message.leaderPublicKey,
        message.certificate,
      );
    });
  }

  private async acceptLeader(
    runtime: GuildRuntime,
    term: number,
    leaderPublicKey: string,
    certificate: SignedElectionVote[],
  ) {
    const leadershipChanged =
      term !== runtime.state.term ||
      leaderPublicKey !== runtime.leaderPublicKey;
    if (
      term > runtime.state.term ||
      runtime.state.votedFor !== leaderPublicKey
    ) {
      runtime.state = { term, votedFor: leaderPublicKey };
      await this.store.putSequencerState(runtime.stateKey, runtime.state);
    }
    if (leadershipChanged) {
      runtime.tokens.clear();
      runtime.activeRequestId = undefined;
    }
    if (leaderPublicKey === this.identity.publicKey) {
      runtime.role = "leader";
    } else {
      this.becomeFollower(runtime);
    }
    runtime.leaderPublicKey = leaderPublicKey;
    runtime.leaderCertificate = certificate;
    if (runtime.role !== "leader") {
      if (this.config.redirectFollowers) {
        this.rejectPendingForRedirect(runtime);
      }
      this.armElection(runtime);
    }
  }

  private redirectError(runtime: GuildRuntime) {
    return new Error(
      `SEQUENCER_REDIRECT ${runtime.leaderPublicKey} ${runtime.state.term}`,
    );
  }

  private rejectPendingForRedirect(runtime: GuildRuntime) {
    const error = this.redirectError(runtime);
    for (const waiters of runtime.waiters.values()) {
      for (const waiter of waiters) {
        clearTimeout(waiter.timer);
        waiter.reject(error);
      }
    }
    runtime.waiters.clear();
    runtime.queued = [];
    runtime.queuedIds.clear();
  }

  private becomeFollower(runtime: GuildRuntime) {
    runtime.role = "follower";
    runtime.leaderPublicKey = undefined;
    runtime.leaderCertificate = undefined;
    runtime.votes.clear();
    runtime.activeRequestId = undefined;
    this.clearHeartbeatTimer(runtime);
  }

  private async selectNext(runtime: GuildRuntime) {
    if (
      this.closed ||
      runtime.role !== "leader" ||
      runtime.activeRequestId ||
      !runtime.leaderCertificate
    ) {
      return;
    }
    let requestId: string | undefined;
    while (runtime.queued.length > 0 && !requestId) {
      const candidate = runtime.queued.shift();
      if (candidate && runtime.queuedIds.delete(candidate)) {
        requestId = candidate;
      }
    }
    if (!requestId) {
      return;
    }
    runtime.activeRequestId = requestId;
    const head = await this.readHead(runtime.guildId);
    const proposal = await this.signMessage({
      protocol: "cgp/sequencer/1",
      kind: "sequencing-proposal",
      epoch: this.config.epoch,
      guildId: runtime.guildId,
      term: runtime.state.term,
      leaderPublicKey: this.identity.publicKey,
      requestId,
      headSeq: head.headSeq,
      headHash: head.headHash,
      certificate: runtime.leaderCertificate,
      selectedAt: Date.now(),
    });
    this.acceptProposal(runtime, proposal);
    try {
      await this.transport.publish(proposal);
    } catch (error) {
      runtime.activeRequestId = undefined;
      this.rejectRequest(
        runtime,
        requestId,
        error instanceof Error
          ? error
          : new Error("Failed to publish sequencer proposal"),
      );
      await this.selectNext(runtime);
    }
  }

  private async observeProposal(
    runtime: GuildRuntime,
    raw: SignedSequencingProposal,
  ) {
    const message = {
      ...raw,
      leaderPublicKey: raw.leaderPublicKey?.toLowerCase(),
    };
    if (
      !this.memberSet.has(message.leaderPublicKey) ||
      !isSafeTerm(message.term) ||
      typeof message.requestId !== "string" ||
      !message.requestId ||
      !isSafeHead(message.headSeq, message.headHash) ||
      !isFreshTimestamp(message.selectedAt) ||
      !this.validLeaderCertificate(
        message.guildId,
        message.term,
        message.leaderPublicKey,
        message.certificate,
      ) ||
      !verify(
        message.leaderPublicKey,
        hashObject(messageSigningPayload(message)),
        message.signature,
      )
    ) {
      return;
    }
    await this.mutate(runtime, async () => {
      if (message.term < runtime.state.term) {
        return;
      }
      await this.acceptLeader(
        runtime,
        message.term,
        message.leaderPublicKey,
        message.certificate,
      );
      this.acceptProposal(runtime, message);
    });
  }

  private acceptProposal(
    runtime: GuildRuntime,
    proposal: SignedSequencingProposal,
  ) {
    // Followers retain their local request queue for failover. Once a certified
    // leader selects a request, mark that queued copy consumed so a future
    // leader cannot replay an already-completed proposal.
    runtime.queuedIds.delete(proposal.requestId);
    const token: RelaySequencingToken = {
      epoch: this.config.epoch,
      guildId: runtime.guildId,
      term: proposal.term,
      leaderPublicKey: proposal.leaderPublicKey,
      requestId: proposal.requestId,
      headSeq: proposal.headSeq,
      headHash: proposal.headHash,
    };
    runtime.tokens.set(proposal.requestId, token);
    if (runtime.tokens.size > 4_000) {
      while (runtime.tokens.size > 3_000) {
        const oldest = runtime.tokens.keys().next().value;
        if (!oldest) {
          break;
        }
        runtime.tokens.delete(oldest);
      }
    }
    const waiters = runtime.waiters.get(proposal.requestId);
    if (!waiters) {
      return;
    }
    runtime.waiters.delete(proposal.requestId);
    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.resolve(token);
    }
  }

  private waitForRequest(runtime: GuildRuntime, requestId: string) {
    const existing = runtime.tokens.get(requestId);
    if (
      existing &&
      existing.term === runtime.state.term &&
      existing.leaderPublicKey === runtime.leaderPublicKey
    ) {
      return Promise.resolve(existing);
    }
    if (existing) {
      runtime.tokens.delete(requestId);
    }
    return new Promise<RelaySequencingToken>((resolve, reject) => {
      const waiters =
        runtime.waiters.get(requestId) ?? new Set<RequestWaiter>();
      const waiter: RequestWaiter = {
        resolve,
        reject,
        timer: setTimeout(() => {
          waiters.delete(waiter);
          if (waiters.size === 0) {
            runtime.waiters.delete(requestId);
          }
          reject(
            new Error(
              `Relay sequencer unavailable for guild ${runtime.guildId} in term ${runtime.state.term}`,
            ),
          );
        }, this.config.requestTimeoutMs),
      };
      waiter.timer.unref?.();
      waiters.add(waiter);
      runtime.waiters.set(requestId, waiters);
    });
  }

  private rejectRequest(
    runtime: GuildRuntime,
    requestId: string,
    error: Error,
  ) {
    const waiters = runtime.waiters.get(requestId);
    if (!waiters) {
      return;
    }
    runtime.waiters.delete(requestId);
    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(error);
    }
  }

  private async signMessage<T extends UnsignedRelaySequencerMessage>(
    unsigned: T,
  ): Promise<T & { signature: string }> {
    return {
      ...unsigned,
      signature: await sign(
        this.identity.privateKey,
        hashObject(unsigned),
      ),
    };
  }
}
