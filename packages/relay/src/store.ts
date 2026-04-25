import { ChannelId, GuildEvent, GuildId, SerializableMember, SerializableMessageRef, UserId } from "@cgp/core";

export interface HistoryQuery {
    guildId: GuildId;
    channelId?: ChannelId;
    beforeSeq?: number;
    afterSeq?: number;
    limit?: number;
    includeStructural?: boolean;
}

export interface SearchEventsQuery extends HistoryQuery {
    scanLimit?: number;
}

export interface ReplaySnapshotQuery {
    guildId: GuildId;
    limit?: number;
}

export interface EventRangeQuery {
    guildId: GuildId;
    afterSeq?: number;
    limit?: number;
}

export interface MemberPageQuery {
    guildId: GuildId;
    afterUserId?: UserId;
    limit?: number;
}

export interface MemberQuery {
    guildId: GuildId;
    userId: UserId;
}

export interface MemberSearchQuery {
    guildId: GuildId;
    query: string;
    limit?: number;
}

export interface MemberPage {
    members: SerializableMember[];
    nextCursor: UserId | null;
    hasMore: boolean;
    totalApprox?: number;
}

export interface MessageRefQuery {
    guildId: GuildId;
    messageId: string;
}

export interface MessageRefsQuery {
    guildId: GuildId;
    messageIds: string[];
}

const DEFAULT_HISTORY_LIMIT = 100;
const MAX_HISTORY_LIMIT = 500;
const DEFAULT_MEMBER_LIMIT = 100;
const MAX_MEMBER_LIMIT = 500;

function normalizeHistoryLimit(limit: unknown) {
    const parsed = Number(limit);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return DEFAULT_HISTORY_LIMIT;
    }
    return Math.min(MAX_HISTORY_LIMIT, Math.floor(parsed));
}

function normalizeSearchScanLimit(limit: unknown) {
    const parsed = Number(limit);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return 5000;
    }
    return Math.min(100000, Math.floor(parsed));
}

function normalizeReplaySnapshotLimit(limit: unknown) {
    const parsed = Number(limit);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return 5000;
    }
    return Math.min(100000, Math.floor(parsed));
}

export function normalizeMemberPageLimit(limit: unknown) {
    const parsed = Number(limit);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return DEFAULT_MEMBER_LIMIT;
    }
    return Math.min(MAX_MEMBER_LIMIT, Math.floor(parsed));
}

function memberSearchTokens(value: unknown) {
    if (typeof value !== "string") return [];
    return value
        .toLowerCase()
        .split(/[^a-z0-9_.-]+/i)
        .map((token) => token.trim())
        .filter((token) => token.length > 0 && token.length <= 128);
}

function memberSearchTerms(member: SerializableMember) {
    const terms = new Set<string>();
    for (const value of [member.userId, member.nickname, member.bio, ...(member.roles ?? [])]) {
        for (const token of memberSearchTokens(value)) {
            terms.add(token);
            if (terms.size >= 48) {
                return terms;
            }
        }
    }
    return terms;
}

function memberMatchesSearch(member: SerializableMember, tokens: string[]) {
    if (tokens.length === 0) return false;
    const haystack = [member.userId, member.nickname, member.bio, ...(member.roles ?? [])]
        .filter((value): value is string => typeof value === "string" && value.length > 0)
        .join(" ")
        .toLowerCase();
    return tokens.every((token) => haystack.includes(token));
}

function eventChannelId(event: GuildEvent) {
    const body = event.body as unknown as Record<string, unknown>;
    return typeof body.channelId === "string" ? body.channelId : "";
}

function eventTargetChannelId(event: GuildEvent) {
    const body = event.body as unknown as Record<string, unknown>;
    const target = body.target && typeof body.target === "object"
        ? body.target as Record<string, unknown>
        : {};
    return typeof target.channelId === "string" ? target.channelId : "";
}

export function eventChannelIds(event: GuildEvent) {
    return [...new Set([eventChannelId(event), eventTargetChannelId(event)].filter(Boolean))];
}

function shouldIncludeStructuralEvent(event: GuildEvent, query: HistoryQuery) {
    if (!query.includeStructural) {
        return false;
    }

    if (event.body.type === "GUILD_CREATE") {
        return true;
    }

    if (event.body.type !== "CHANNEL_CREATE") {
        return false;
    }

    return !query.channelId || eventChannelId(event) === query.channelId;
}

function matchesHistoryQuery(event: GuildEvent, query: HistoryQuery) {
    if (query.beforeSeq !== undefined && event.seq >= query.beforeSeq) {
        return false;
    }

    if (query.afterSeq !== undefined && event.seq <= query.afterSeq) {
        return false;
    }

    if (!query.channelId) {
        return true;
    }

    if (eventChannelId(event) === query.channelId || eventTargetChannelId(event) === query.channelId) {
        return true;
    }

    return shouldIncludeStructuralEvent(event, query);
}

export function selectHistoryEvents(log: GuildEvent[], query: HistoryQuery): GuildEvent[] {
    const limit = normalizeHistoryLimit(query.limit);
    if (query.afterSeq !== undefined) {
        const events: GuildEvent[] = [];
        for (const event of log) {
            if (!matchesHistoryQuery(event, query)) {
                continue;
            }
            events.push(event);
            if (events.length >= limit) {
                break;
            }
        }
        return events;
    }

    const events: GuildEvent[] = [];
    for (let index = log.length - 1; index >= 0; index -= 1) {
        const event = log[index];
        if (!event || !matchesHistoryQuery(event, query)) {
            continue;
        }
        events.push(event);
        if (events.length >= limit) {
            break;
        }
    }
    events.reverse();
    return events;
}

export function selectSearchCandidateEvents(log: GuildEvent[], query: SearchEventsQuery): GuildEvent[] {
    const scanLimit = normalizeSearchScanLimit(query.scanLimit);
    const events: GuildEvent[] = [];

    if (query.afterSeq !== undefined) {
        for (const event of log) {
            if (!matchesHistoryQuery(event, query)) {
                continue;
            }
            events.push(event);
            if (events.length >= scanLimit) {
                break;
            }
        }
        return events;
    }

    for (let index = log.length - 1; index >= 0; index -= 1) {
        const event = log[index];
        if (!event || !matchesHistoryQuery(event, query)) {
            continue;
        }
        events.push(event);
        if (events.length >= scanLimit) {
            break;
        }
    }
    events.reverse();
    return events;
}

export function selectReplaySnapshotEvents(log: GuildEvent[], query: ReplaySnapshotQuery): GuildEvent[] {
    const limit = normalizeReplaySnapshotLimit(query.limit);
    const events: GuildEvent[] = [];

    for (let index = log.length - 1; index >= 0; index -= 1) {
        const event = log[index];
        if (!event) {
            continue;
        }
        events.push(event);
        if (event.body.type === "CHECKPOINT" || events.length >= limit) {
            break;
        }
    }

    events.reverse();
    return events;
}

export interface Store {
    getLog(guildId: GuildId): Promise<GuildEvent[]> | GuildEvent[];
    iterateLog?(guildId: GuildId): AsyncIterable<GuildEvent> | Iterable<GuildEvent>;
    getEventRange?(query: EventRangeQuery): Promise<GuildEvent[]> | GuildEvent[];
    getHistory?(query: HistoryQuery): Promise<GuildEvent[]> | GuildEvent[];
    getSearchEvents?(query: SearchEventsQuery): Promise<GuildEvent[]> | GuildEvent[];
    getReplaySnapshotEvents?(query: ReplaySnapshotQuery): Promise<GuildEvent[]> | GuildEvent[];
    getMembersPage?(query: MemberPageQuery): Promise<MemberPage> | MemberPage;
    getMember?(query: MemberQuery): Promise<SerializableMember | null> | SerializableMember | null;
    searchMembers?(query: MemberSearchQuery): Promise<MemberPage> | MemberPage;
    getMessageRef?(query: MessageRefQuery): Promise<SerializableMessageRef | null> | SerializableMessageRef | null;
    getMessageRefs?(query: MessageRefsQuery): Promise<Map<string, SerializableMessageRef>> | Map<string, SerializableMessageRef>;
    append(guildId: GuildId, event: GuildEvent): Promise<void> | void;
    appendEvents?(guildId: GuildId, events: GuildEvent[]): Promise<void> | void;
    getLastEvent(guildId: GuildId): Promise<GuildEvent | undefined> | GuildEvent | undefined;
    getGuildIds(): Promise<GuildId[]> | GuildId[];
    deleteEvent(guildId: GuildId, seq: number): Promise<void> | void;
    deleteEvents?(guildId: GuildId, seqs: number[]): Promise<void> | void;
    compact?(query?: { start?: string; end?: string }): Promise<void> | void;
    close(): Promise<void> | void;
}

export class MemoryStore implements Store {
    private logs = new Map<GuildId, GuildEvent[]>();
    private channelLogs = new Map<GuildId, Map<ChannelId, GuildEvent[]>>();
    private members = new Map<GuildId, Map<UserId, SerializableMember>>();
    private memberOrder = new Map<GuildId, UserId[]>();
    private memberSearchIndex = new Map<GuildId, Map<string, Set<UserId>>>();
    private memberSearchOrder = new Map<GuildId, string[]>();
    private memberSearchDocs = new Map<GuildId, Map<UserId, Set<string>>>();
    private messageRefs = new Map<GuildId, Map<string, SerializableMessageRef>>();
    private channelMessageRefs = new Map<GuildId, Map<ChannelId, Set<string>>>();

    getLog(guildId: GuildId): GuildEvent[] {
        return this.logs.get(guildId) || [];
    }

    *iterateLog(guildId: GuildId): Iterable<GuildEvent> {
        yield* this.getLog(guildId);
    }

    getEventRange(query: EventRangeQuery): GuildEvent[] {
        const limit = Math.min(10000, Math.max(1, Math.floor(Number(query.limit) || 5000)));
        const events: GuildEvent[] = [];
        for (const event of this.getLog(query.guildId)) {
            if (query.afterSeq !== undefined && event.seq <= query.afterSeq) {
                continue;
            }
            events.push(event);
            if (events.length >= limit) {
                break;
            }
        }
        return events;
    }

    getHistory(query: HistoryQuery): GuildEvent[] {
        if (query.channelId && !query.includeStructural) {
            return selectHistoryEvents(this.channelEvents(query.guildId, query.channelId), query);
        }
        return selectHistoryEvents(this.getLog(query.guildId), query);
    }

    getSearchEvents(query: SearchEventsQuery): GuildEvent[] {
        if (query.channelId && !query.includeStructural) {
            return selectSearchCandidateEvents(this.channelEvents(query.guildId, query.channelId), query);
        }
        return selectSearchCandidateEvents(this.getLog(query.guildId), query);
    }

    getReplaySnapshotEvents(query: ReplaySnapshotQuery): GuildEvent[] {
        return selectReplaySnapshotEvents(this.getLog(query.guildId), query);
    }

    getMembersPage(query: MemberPageQuery): MemberPage {
        const limit = normalizeMemberPageLimit(query.limit);
        const members = this.members.get(query.guildId) || new Map<UserId, SerializableMember>();
        const order = this.memberOrder.get(query.guildId) || [];
        const pageStart = query.afterUserId ? this.firstMemberAfter(order, query.afterUserId) : 0;
        const page = order
            .slice(pageStart, pageStart + limit + 1)
            .map((userId) => members.get(userId))
            .filter((member): member is SerializableMember => Boolean(member));
        const hasMore = page.length > limit;
        const visible = hasMore ? page.slice(0, limit) : page;
        return {
            members: visible,
            nextCursor: hasMore && visible.length > 0 ? visible[visible.length - 1].userId : null,
            hasMore,
            totalApprox: order.length
        };
    }

    getMember(query: MemberQuery): SerializableMember | null {
        return this.members.get(query.guildId)?.get(query.userId) ?? null;
    }

    searchMembers(query: MemberSearchQuery): MemberPage {
        const limit = normalizeMemberPageLimit(query.limit);
        const tokens = memberSearchTokens(query.query);
        const firstToken = tokens[0];
        if (!firstToken) {
            return { members: [], nextCursor: null, hasMore: false };
        }

        const members = this.members.get(query.guildId) || new Map<UserId, SerializableMember>();
        const index = this.memberSearchIndex.get(query.guildId) || new Map<string, Set<UserId>>();
        const terms = this.memberSearchOrder.get(query.guildId) || [];
        const seen = new Set<UserId>();
        const matched: SerializableMember[] = [];

        for (let termIndex = this.firstTermAtOrAfter(terms, firstToken); termIndex < terms.length; termIndex += 1) {
            const term = terms[termIndex];
            if (!term.startsWith(firstToken)) {
                break;
            }
            for (const userId of index.get(term) || []) {
                if (seen.has(userId)) {
                    continue;
                }
                seen.add(userId);
                const member = members.get(userId);
                if (!member || !memberMatchesSearch(member, tokens)) {
                    continue;
                }
                matched.push(member);
                if (matched.length >= limit + 1) {
                    const visible = matched.slice(0, limit);
                    return {
                        members: visible,
                        nextCursor: visible.length > 0 ? visible[visible.length - 1].userId : null,
                        hasMore: true,
                        totalApprox: members.size
                    };
                }
            }
        }

        return {
            members: matched,
            nextCursor: null,
            hasMore: false,
            totalApprox: members.size
        };
    }

    getMessageRef(query: MessageRefQuery): SerializableMessageRef | null {
        return this.messageRefs.get(query.guildId)?.get(query.messageId) ?? null;
    }

    getMessageRefs(query: MessageRefsQuery): Map<string, SerializableMessageRef> {
        const refs = new Map<string, SerializableMessageRef>();
        const guildRefs = this.messageRefs.get(query.guildId);
        if (!guildRefs) {
            return refs;
        }
        for (const messageId of new Set(query.messageIds)) {
            const ref = guildRefs.get(messageId);
            if (ref) {
                refs.set(messageId, ref);
            }
        }
        return refs;
    }

    append(guildId: GuildId, event: GuildEvent) {
        this.appendEvents(guildId, [event]);
    }

    appendEvents(guildId: GuildId, events: GuildEvent[]) {
        if (events.length === 0) {
            return;
        }
        const log = this.logs.get(guildId) || [];
        const guildChannels = this.channelLogs.get(guildId) || new Map<ChannelId, GuildEvent[]>();

        for (const event of events) {
            log.push(event);
            for (const channelId of eventChannelIds(event)) {
                const channelLog = guildChannels.get(channelId) || [];
                channelLog.push(event);
                guildChannels.set(channelId, channelLog);
            }
            this.applyDerivedIndexes(guildId, event);
        }

        this.logs.set(guildId, log);
        if (guildChannels.size > 0) {
            this.channelLogs.set(guildId, guildChannels);
        }
    }

    getLastEvent(guildId: GuildId): GuildEvent | undefined {
        const log = this.logs.get(guildId);
        if (!log || log.length === 0) return undefined;
        return log[log.length - 1];
    }

    getGuildIds(): GuildId[] {
        return Array.from(this.logs.keys());
    }

    deleteEvent(guildId: GuildId, seq: number) {
        this.deleteEvents(guildId, [seq]);
    }

    deleteEvents(guildId: GuildId, seqs: number[]) {
        const log = this.logs.get(guildId);
        if (!log) return;
        const deleteSet = new Set(seqs.filter((seq) => Number.isInteger(seq)));
        if (deleteSet.size === 0) return;

        let writeIndex = 0;
        for (let readIndex = 0; readIndex < log.length; readIndex += 1) {
            const event = log[readIndex];
            if (deleteSet.has(event.seq)) {
                continue;
            }
            log[writeIndex++] = event;
        }

        if (writeIndex !== log.length) {
            log.length = writeIndex;
            this.rebuildGuildIndexes(guildId);
        }
    }

    close() {
        // No-op
    }

    private channelEvents(guildId: GuildId, channelId: ChannelId) {
        return this.channelLogs.get(guildId)?.get(channelId) || [];
    }

    private rebuildGuildIndexes(guildId: GuildId) {
        const guildChannels = new Map<ChannelId, GuildEvent[]>();
        this.members.delete(guildId);
        this.memberOrder.delete(guildId);
        this.memberSearchIndex.delete(guildId);
        this.memberSearchOrder.delete(guildId);
        this.memberSearchDocs.delete(guildId);
        this.messageRefs.delete(guildId);
        this.channelMessageRefs.delete(guildId);
        for (const event of this.logs.get(guildId) || []) {
            for (const channelId of eventChannelIds(event)) {
                const channelLog = guildChannels.get(channelId) || [];
                channelLog.push(event);
                guildChannels.set(channelId, channelLog);
            }
            this.applyDerivedIndexes(guildId, event);
        }
        if (guildChannels.size === 0) {
            this.channelLogs.delete(guildId);
        } else {
            this.channelLogs.set(guildId, guildChannels);
        }
    }

    private guildMembers(guildId: GuildId) {
        const members = this.members.get(guildId) || new Map<UserId, SerializableMember>();
        this.members.set(guildId, members);
        return members;
    }

    private guildMemberOrder(guildId: GuildId) {
        const order = this.memberOrder.get(guildId) || [];
        this.memberOrder.set(guildId, order);
        return order;
    }

    private guildMemberSearchIndex(guildId: GuildId) {
        const index = this.memberSearchIndex.get(guildId) || new Map<string, Set<UserId>>();
        this.memberSearchIndex.set(guildId, index);
        return index;
    }

    private guildMemberSearchOrder(guildId: GuildId) {
        const order = this.memberSearchOrder.get(guildId) || [];
        this.memberSearchOrder.set(guildId, order);
        return order;
    }

    private guildMemberSearchDocs(guildId: GuildId) {
        const docs = this.memberSearchDocs.get(guildId) || new Map<UserId, Set<string>>();
        this.memberSearchDocs.set(guildId, docs);
        return docs;
    }

    private firstMemberAfter(order: UserId[], userId: UserId) {
        let low = 0;
        let high = order.length;
        while (low < high) {
            const mid = (low + high) >> 1;
            if (order[mid].localeCompare(userId) <= 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private insertMemberOrder(order: UserId[], userId: UserId) {
        const index = this.firstMemberAfter(order, userId);
        if (order[index - 1] === userId || order[index] === userId) {
            return;
        }
        order.splice(index, 0, userId);
    }

    private firstTermAtOrAfter(order: string[], term: string) {
        let low = 0;
        let high = order.length;
        while (low < high) {
            const mid = (low + high) >> 1;
            if (order[mid].localeCompare(term) < 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    private insertMemberSearchTerm(order: string[], term: string) {
        const index = this.firstTermAtOrAfter(order, term);
        if (order[index] === term) {
            return;
        }
        order.splice(index, 0, term);
    }

    private deleteMemberSearchTerm(order: string[], term: string) {
        const index = this.firstTermAtOrAfter(order, term);
        if (order[index] === term) {
            order.splice(index, 1);
        }
    }

    private deleteMemberSearchKeys(guildId: GuildId, userId: UserId) {
        const docs = this.guildMemberSearchDocs(guildId);
        const previousTerms = docs.get(userId);
        if (!previousTerms) {
            return;
        }

        const index = this.guildMemberSearchIndex(guildId);
        const order = this.guildMemberSearchOrder(guildId);
        for (const term of previousTerms) {
            const users = index.get(term);
            if (!users) {
                continue;
            }
            users.delete(userId);
            if (users.size === 0) {
                index.delete(term);
                this.deleteMemberSearchTerm(order, term);
            }
        }
        docs.delete(userId);
    }

    private indexMemberForSearch(guildId: GuildId, member: SerializableMember) {
        this.deleteMemberSearchKeys(guildId, member.userId);
        const terms = memberSearchTerms(member);
        this.guildMemberSearchDocs(guildId).set(member.userId, terms);
        const index = this.guildMemberSearchIndex(guildId);
        const order = this.guildMemberSearchOrder(guildId);
        for (const term of terms) {
            const users = index.get(term) || new Set<UserId>();
            users.add(member.userId);
            index.set(term, users);
            this.insertMemberSearchTerm(order, term);
        }
    }

    private setIndexedMember(guildId: GuildId, members: Map<UserId, SerializableMember>, member: SerializableMember) {
        if (!members.has(member.userId)) {
            this.insertMemberOrder(this.guildMemberOrder(guildId), member.userId);
        }
        members.set(member.userId, member);
        this.indexMemberForSearch(guildId, member);
    }

    private deleteIndexedMember(guildId: GuildId, members: Map<UserId, SerializableMember>, userId: UserId) {
        if (!members.delete(userId)) {
            return;
        }
        this.deleteMemberSearchKeys(guildId, userId);
        const order = this.memberOrder.get(guildId);
        if (!order) {
            return;
        }
        const index = this.firstMemberAfter(order, userId) - 1;
        if (index >= 0 && order[index] === userId) {
            order.splice(index, 1);
        }
    }

    private guildMessageRefs(guildId: GuildId) {
        const refs = this.messageRefs.get(guildId) || new Map<string, SerializableMessageRef>();
        this.messageRefs.set(guildId, refs);
        return refs;
    }

    private guildChannelMessageRefs(guildId: GuildId) {
        const refs = this.channelMessageRefs.get(guildId) || new Map<ChannelId, Set<string>>();
        this.channelMessageRefs.set(guildId, refs);
        return refs;
    }

    private applyDerivedIndexes(guildId: GuildId, event: GuildEvent) {
        const body = event.body as Record<string, any>;
        const members = this.guildMembers(guildId);
        const messageRefs = this.guildMessageRefs(guildId);
        const channelMessageRefs = this.guildChannelMessageRefs(guildId);

        switch (body.type) {
            case "GUILD_CREATE":
                this.setIndexedMember(guildId, members, {
                    userId: event.author,
                    roles: ["owner"],
                    joinedAt: event.createdAt
                });
                break;
            case "MEMBER_UPDATE": {
                const userId = typeof body.userId === "string" && body.userId.trim() ? body.userId : event.author;
                const current = members.get(userId) || { userId, roles: [], joinedAt: event.createdAt };
                this.setIndexedMember(guildId, members, {
                    ...current,
                    nickname: typeof body.nickname === "string" ? body.nickname : current.nickname,
                    avatar: typeof body.avatar === "string" ? body.avatar : current.avatar,
                    banner: typeof body.banner === "string" ? body.banner : current.banner,
                    bio: typeof body.bio === "string" ? body.bio : current.bio
                });
                break;
            }
            case "ROLE_ASSIGN": {
                if (typeof body.userId !== "string" || typeof body.roleId !== "string") break;
                const current = members.get(body.userId) || { userId: body.userId, roles: [], joinedAt: event.createdAt };
                this.setIndexedMember(guildId, members, {
                    ...current,
                    roles: Array.from(new Set([...current.roles, body.roleId])).sort()
                });
                break;
            }
            case "ROLE_REVOKE": {
                if (typeof body.userId !== "string" || typeof body.roleId !== "string") break;
                const current = members.get(body.userId);
                if (current) {
                    this.setIndexedMember(guildId, members, {
                        ...current,
                        roles: current.roles.filter((roleId) => roleId !== body.roleId)
                    });
                }
                break;
            }
            case "ROLE_DELETE": {
                if (typeof body.roleId !== "string") break;
                for (const [userId, member] of members) {
                    if (member.roles.includes(body.roleId)) {
                        this.setIndexedMember(guildId, members, {
                            ...member,
                            roles: member.roles.filter((roleId) => roleId !== body.roleId)
                        });
                    }
                }
                break;
            }
            case "BAN_USER":
            case "BAN_ADD":
            case "MEMBER_KICK":
                if (typeof body.userId === "string") {
                    this.deleteIndexedMember(guildId, members, body.userId);
                }
                break;
            case "MESSAGE": {
                const messageId = typeof body.messageId === "string" && body.messageId.trim() ? body.messageId : event.id;
                if (typeof body.channelId !== "string") break;
                messageRefs.set(messageId, {
                    channelId: body.channelId,
                    authorId: event.author
                });
                const channelRefs = channelMessageRefs.get(body.channelId) || new Set<string>();
                channelRefs.add(messageId);
                channelMessageRefs.set(body.channelId, channelRefs);
                break;
            }
            case "REACTION_ADD": {
                this.updateReaction(guildId, body.messageId, body.reaction, event.author, true);
                break;
            }
            case "REACTION_REMOVE": {
                const removeUserId = typeof body.userId === "string" && body.userId.trim() ? body.userId : event.author;
                this.updateReaction(guildId, body.messageId, body.reaction, removeUserId, false);
                break;
            }
            case "DELETE_MESSAGE": {
                const ref = typeof body.messageId === "string" ? messageRefs.get(body.messageId) : undefined;
                if (ref) {
                    messageRefs.set(body.messageId, { ...ref, deleted: true });
                }
                break;
            }
            case "CHANNEL_DELETE": {
                if (typeof body.channelId !== "string") break;
                for (const messageId of channelMessageRefs.get(body.channelId) || []) {
                    const ref = messageRefs.get(messageId);
                    if (ref) {
                        messageRefs.set(messageId, { ...ref, deleted: true });
                    }
                }
                break;
            }
        }
    }

    private updateReaction(guildId: GuildId, messageId: unknown, reaction: unknown, userId: string, add: boolean) {
        if (typeof messageId !== "string" || typeof reaction !== "string" || !reaction.trim()) {
            return;
        }
        const refs = this.guildMessageRefs(guildId);
        const ref = refs.get(messageId);
        if (!ref) {
            return;
        }
        const reactions = { ...(ref.reactions ?? {}) };
        const users = new Set(reactions[reaction] ?? []);
        if (add) {
            users.add(userId);
        } else {
            users.delete(userId);
        }
        if (users.size > 0) {
            reactions[reaction] = Array.from(users).sort();
        } else {
            delete reactions[reaction];
        }
        refs.set(messageId, {
            ...ref,
            reactions: Object.keys(reactions).length > 0 ? reactions : undefined
        });
    }
}
