import { randomUUID } from 'node:crypto';
import { WebSocket } from 'ws';
import {
    consensusCommitHash, consensusMigrationHash, computeEventId, hashObject, normalizeConsensusPolicy, verifyConsensusHistory,
    verifyLegacyConsensusRequest, legacyConsensusRequestPayload, consensusUnsigned, verify,
    encodeCgpFrame, socketDataToCgpFrame,
    type ConsensusHistory, type ConsensusPolicy, type ConsensusReadPayload,
    type ConsensusRequestAuthorization, type ConsensusValue, type ConsensusTransition,
    type GuildEvent, type LegacyConsensusTrust, type LegacyConsensusMigrationRequest, type LegacyConsensusFreeze,
} from '@cgp/core';

export interface ConsensusV2ClientOptions {
    relayUrl: string;
    guildId: string;
    /** Explicit trusted guild anchor, obtained outside this relay connection. */
    anchorPolicy: ConsensusPolicy;
    /** Additional explicit old-policy trust required only for a certified legacy bridge. */
    legacyTrust?: LegacyConsensusTrust;
    /** Sign this exact read payload; never derive authorization/trust from HELLO. */
    readSigner(payload: ConsensusReadPayload): Promise<Omit<ConsensusRequestAuthorization, 'createdAt'>>;
    timeoutMs?: number;
}

/** Opt-in certified history transport; does not migrate or mutate legacy client state. */
export class ConsensusV2Client {
    private socket?: WebSocket;
    private connecting?: Promise<void>;
    private closed = false;
    private anchor: ConsensusPolicy;
    private legacyTrust?: LegacyConsensusTrust;
    private history?: ConsensusHistory;
    private serial: Promise<unknown> = Promise.resolve();
    private pending = new Map<string, {
        resolve(value: unknown): void;
        reject(error: Error): void;
        timer: ReturnType<typeof setTimeout>;
        expectedKind: string;
    }>();
    private timeout: number;

    constructor(private readonly options: ConsensusV2ClientOptions) {
        if (!options.guildId?.trim()) throw Error('A guild ID is required');
        const url = new URL(options.relayUrl);
        if (!['ws:', 'wss:'].includes(url.protocol) || url.username || url.password) throw Error('Invalid relay URL');
        this.anchor = normalizeConsensusPolicy(structuredClone(options.anchorPolicy));
        this.legacyTrust = options.legacyTrust ? structuredClone(options.legacyTrust) : undefined;
        this.timeout = Math.max(100, Math.min(60_000, options.timeoutMs ?? 15_000));
    }

    async connect(): Promise<void> {
        if (this.closed) throw Error('Consensus client is closed');
        if (this.socket?.readyState === WebSocket.OPEN) return;
        if (this.connecting) return this.connecting;
        const socket = new WebSocket(this.options.relayUrl, { maxPayload: 16 * 1024 * 1024 });
        this.socket = socket;
        socket.on('message', data => { void this.receive(data); });
        socket.on('close', () => this.rejectPending(Error('Consensus transport closed')));
        socket.on('error', error => this.rejectPending(error));
        this.connecting = new Promise<void>((resolve, reject) => {
            const timer = setTimeout(() => { socket.terminate(); reject(Error('Consensus connection timed out')); }, this.timeout);
            socket.once('open', () => {
                clearTimeout(timer);
                socket.send(encodeCgpFrame('HELLO', { protocol: 'cgp/0.1' }, 'json'));
                resolve();
            });
            socket.once('error', error => { clearTimeout(timer); reject(error); });
            socket.once('close', () => { clearTimeout(timer); reject(Error('Consensus transport closed')); });
        }).finally(() => { this.connecting = undefined; });
        return this.connecting;
    }

    fetchHistory() {
        return this.enqueue(async () => this.accept(await this.request('CONSENSUS_HISTORY')));
    }

    submitEvent(event: GuildEvent) { return this.submit({ kind: 'event', event: structuredClone(event) }); }
    submitTransition(transition: ConsensusTransition) { return this.submit(structuredClone(transition)); }

    /** Irreversibly freezes this old voter for the signed migration. Operator-only opt-in. */
    freezeLegacy(migration: LegacyConsensusMigrationRequest, expectedVoter: string) {
        const request = structuredClone(migration);
        return this.enqueue(async () => {
            if (!this.legacyTrust || !this.legacyTrust.policy.members.includes(expectedVoter) ||
                !verifyLegacyConsensusRequest(request, this.options.guildId, this.anchor, this.legacyTrust)) {
                throw Error('Explicit legacy trust and authorized migration are required');
            }
            const freeze = await this.request<LegacyConsensusFreeze>('CONSENSUS_FREEZE', undefined, { migration: request });
            if (freeze.protocol !== 'cgp/legacy-freeze/2' || freeze.guildId !== this.options.guildId ||
                freeze.relayPublicKey !== expectedVoter || freeze.requestHash !== hashObject(legacyConsensusRequestPayload(request)) ||
                !Number.isSafeInteger(freeze.headSeq) || freeze.headSeq < -1 ||
                (freeze.headHash !== null && !/^[a-f0-9]{64}$/.test(freeze.headHash)) ||
                (freeze.fenceProposalId !== null && !/^[a-f0-9]{64}$/.test(freeze.fenceProposalId)) ||
                !verify(expectedVoter, hashObject(consensusUnsigned(freeze)), freeze.signature)) {
                throw Error('Invalid signed legacy freeze receipt');
            }
            return freeze;
        });
    }

    importHistory(history: ConsensusHistory) {
        const candidate = structuredClone(history);
        return this.enqueue(async () => {
            this.accept(candidate, false);
            const response = await this.request('CONSENSUS_IMPORT', undefined, { history: candidate });
            const result = this.accept(response, false);
            this.assertExtension(candidate, response);
            this.history = structuredClone(response);
            return result;
        });
    }

    private submit(value: ConsensusValue) {
        return this.enqueue(async () => {
            // Establish a certified prefix before interpreting a recovered/chosen value.
            if (!this.history) this.accept(await this.request('CONSENSUS_HISTORY'));
            const previousLength = this.history!.entries.length;
            const known = this.accept(this.history!);
            if (this.contains(known, value)) return { ...known, requestedCommitted: true, appended: [] };
            const result = this.accept(await this.request('CONSENSUS_PUBLISH', value));
            return {
                ...result,
                // Consensus may recover an older accepted value instead of this request.
                requestedCommitted: this.contains(result, value),
                appended: result.history.entries.slice(previousLength),
            };
        });
    }

    private contains(result: ReturnType<ConsensusV2Client['accept']>, value: ConsensusValue) {
        return value.kind === 'event'
            ? value.event.id === computeEventId(value.event) && result.verified.events.some(event => event.id === value.event.id)
            : result.history.entries.some(entry => hashObject(entry.commit.value) === hashObject(value));
    }

    private enqueue<T>(operation: () => Promise<T>): Promise<T> {
        const result = this.serial.then(operation, operation);
        this.serial = result.catch(() => undefined);
        return result;
    }

    private accept(history: ConsensusHistory, remember = true) {
        const verified = verifyConsensusHistory(history, this.options.guildId, this.anchor, this.legacyTrust);
        if (this.history) this.assertExtension(this.history, history);
        if (remember) this.history = structuredClone(history);
        return { history: structuredClone(history), verified };
    }

    private assertExtension(previousHistory: ConsensusHistory, history: ConsensusHistory) {
            const priorBase = previousHistory.base ? consensusMigrationHash(previousHistory.base) : null;
            const nextBase = history.base ? consensusMigrationHash(history.base) : null;
            const initialMigration = !!this.legacyTrust && priorBase === null && previousHistory.entries.length === 0 && nextBase !== null;
            if (priorBase !== nextBase && !initialMigration) throw Error('Consensus migration base changed');
            if (history.entries.length < previousHistory.entries.length) throw Error('Consensus history rollback');
            for (let i = 0; i < previousHistory.entries.length; i++) {
                const previous = previousHistory.entries[i], next = history.entries[i];
                if (consensusCommitHash(previous.commit) !== consensusCommitHash(next.commit)) throw Error('Consensus history fork');
                if (previous.activation && !next.activation) throw Error('Consensus activation rollback');
            }
    }

    private async request<T = ConsensusHistory>(kind: 'CONSENSUS_HISTORY' | 'CONSENSUS_PUBLISH' | 'CONSENSUS_FREEZE' | 'CONSENSUS_IMPORT', value?: ConsensusValue, extra: Record<string, unknown> = {}): Promise<T> {
        await this.connect();
        const requestId = randomUUID(), createdAt = Date.now();
        const payload: ConsensusReadPayload = { protocol: 'cgp/consensus-read/2', guildId: this.options.guildId, requestId, createdAt };
        const authorization = await this.options.readSigner(payload);
        if (!authorization.author || !authorization.signature) throw Error('Signed consensus read authorization required');
        if (this.closed || this.socket?.readyState !== WebSocket.OPEN) throw Error('Consensus transport unavailable');
        const frame = { ...authorization, ...extra, guildId: this.options.guildId, requestId, createdAt, ...(value ? { value } : {}) };
        return new Promise<T>((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pending.delete(requestId);
                reject(Error('Consensus request timed out; commit outcome is unknown, fetch certified history before retrying'));
            }, this.timeout);
            this.pending.set(requestId, { resolve: result => resolve(result as T), reject, timer, expectedKind: kind === 'CONSENSUS_FREEZE' ? 'CONSENSUS_FROZEN' : 'CONSENSUS_RESULT' });
            this.socket!.send(encodeCgpFrame(kind, frame, 'json'), error => {
                if (error) this.finish(requestId, error);
            });
        });
    }

    private async receive(data: WebSocket.RawData) {
        try {
            const { kind, payload } = await socketDataToCgpFrame(data);
            if (kind !== 'CONSENSUS_RESULT' && kind !== 'CONSENSUS_ERROR' && kind !== 'CONSENSUS_FROZEN') return;
            const response = payload as { requestId?: string; guildId?: string; history?: ConsensusHistory; freeze?: LegacyConsensusFreeze; code?: string; message?: string };
            if (!response || typeof response.requestId !== 'string' || !this.pending.has(response.requestId)) return;
            if (response.guildId !== this.options.guildId) return this.finish(response.requestId, Error('Consensus response guild mismatch'));
            if (kind === 'CONSENSUS_ERROR') return this.finish(response.requestId, Error(`${response.code ?? 'CONSENSUS_ERROR'}: ${response.message ?? 'Request failed'}`));
            if (kind !== this.pending.get(response.requestId)!.expectedKind) return this.finish(response.requestId, Error('Unexpected consensus response type'));
            if (kind === 'CONSENSUS_FROZEN') {
                return response.freeze ? this.finish(response.requestId, undefined, response.freeze) : this.finish(response.requestId, Error('Missing freeze receipt'));
            }
            if (!response.history) return this.finish(response.requestId, Error('Missing certified consensus history'));
            this.finish(response.requestId, undefined, response.history);
        } catch (error) {
            this.rejectPending(error instanceof Error ? error : Error('Invalid consensus frame'));
        }
    }

    private finish(id: string, error?: Error, result?: unknown) {
        const waiter = this.pending.get(id);
        if (!waiter) return;
        clearTimeout(waiter.timer);
        this.pending.delete(id);
        if (error) waiter.reject(error); else waiter.resolve(result);
    }
    private rejectPending(error: Error) { for (const id of this.pending.keys()) this.finish(id, error); }
    close() {
        this.closed = true;
        this.rejectPending(Error('Consensus client closed'));
        this.socket?.terminate();
    }
}
