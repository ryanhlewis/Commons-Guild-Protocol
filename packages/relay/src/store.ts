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

export interface MemberPageQuery {
    guildId: GuildId;
    afterUserId?: UserId;
    limit?: number;
}

export interface MemberQuery {
    guildId: GuildId;
    userId: UserId;
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
    getHistory?(query: HistoryQuery): Promise<GuildEvent[]> | GuildEvent[];
    getSearchEvents?(query: SearchEventsQuery): Promise<GuildEvent[]> | GuildEvent[];
    getReplaySnapshotEvents?(query: ReplaySnapshotQuery): Promise<GuildEvent[]> | GuildEvent[];
    getMembersPage?(query: MemberPageQuery): Promise<MemberPage> | MemberPage;
    getMember?(query: MemberQuery): Promise<SerializableMember | null> | SerializableMember | null;
    getMessageRef?(query: MessageRefQuery): Promise<SerializableMessageRef | null> | SerializableMessageRef | null;
    append(guildId: GuildId, event: GuildEvent): Promise<void> | void;
    getLastEvent(guildId: GuildId): Promise<GuildEvent | undefined> | GuildEvent | undefined;
    getGuildIds(): Promise<GuildId[]> | GuildId[];
    deleteEvent(guildId: GuildId, seq: number): Promise<void> | void;
    close(): Promise<void> | void;
}

export class MemoryStore implements Store {
    private logs = new Map<GuildId, GuildEvent[]>();
    private channelLogs = new Map<GuildId, Map<ChannelId, GuildEvent[]>>();
    private members = new Map<GuildId, Map<UserId, SerializableMember>>();
    private memberOrder = new Map<GuildId, UserId[]>();
    private messageRefs = new Map<GuildId, Map<string, SerializableMessageRef>>();
    private channelMessageRefs = new Map<GuildId, Map<ChannelId, Set<string>>>();

    getLog(guildId: GuildId): GuildEvent[] {
        return this.logs.get(guildId) || [];
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

    getMessageRef(query: MessageRefQuery): SerializableMessageRef | null {
        return this.messageRefs.get(query.guildId)?.get(query.messageId) ?? null;
    }

    append(guildId: GuildId, event: GuildEvent) {
        const log = this.logs.get(guildId) || [];
        log.push(event);
        this.logs.set(guildId, log);
        for (const channelId of eventChannelIds(event)) {
            const guildChannels = this.channelLogs.get(guildId) || new Map<ChannelId, GuildEvent[]>();
            const channelLog = guildChannels.get(channelId) || [];
            channelLog.push(event);
            guildChannels.set(channelId, channelLog);
            this.channelLogs.set(guildId, guildChannels);
        }
        this.applyDerivedIndexes(guildId, event);
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
        const log = this.logs.get(guildId);
        if (!log) return;
        const index = log.findIndex(e => e.seq === seq);
        if (index !== -1) {
            log.splice(index, 1);
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

    private setIndexedMember(guildId: GuildId, members: Map<UserId, SerializableMember>, member: SerializableMember) {
        if (!members.has(member.userId)) {
            this.insertMemberOrder(this.guildMemberOrder(guildId), member.userId);
        }
        members.set(member.userId, member);
    }

    private deleteIndexedMember(guildId: GuildId, members: Map<UserId, SerializableMember>, userId: UserId) {
        if (!members.delete(userId)) {
            return;
        }
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
                    members.set(body.userId, {
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
                        members.set(userId, {
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
