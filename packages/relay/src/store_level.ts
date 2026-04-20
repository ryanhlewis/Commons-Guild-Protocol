import { Level } from "level";
import { GuildEvent, GuildId, SerializableMember, SerializableMessageRef } from "@cgp/core";
import {
    eventChannelIds,
    HistoryQuery,
    MemberQuery,
    MemberPage,
    MemberPageQuery,
    MessageRefQuery,
    normalizeMemberPageLimit,
    ReplaySnapshotQuery,
    SearchEventsQuery,
    selectHistoryEvents,
    Store
} from "./store";

const MIN_SEQ_KEY = "0000000000";
const MAX_SEQ_KEY = "9999999999";

function seqKey(seq: number) {
    return seq.toString().padStart(10, "0");
}

function guildSeqKey(guildId: GuildId, seq: number) {
    return `guild:${guildId}:seq:${seqKey(seq)}`;
}

function guildChannelSeqKey(guildId: GuildId, channelId: string, seq: number) {
    return `guild:${guildId}:chan:${encodeURIComponent(channelId)}:seq:${seqKey(seq)}`;
}

function guildChannelSeqPrefix(guildId: GuildId, channelId: string) {
    return `guild:${guildId}:chan:${encodeURIComponent(channelId)}:seq:`;
}

function guildMemberKey(guildId: GuildId, userId: string) {
    return `guild:${guildId}:member:${encodeURIComponent(userId)}`;
}

function guildMemberPrefix(guildId: GuildId) {
    return `guild:${guildId}:member:`;
}

function guildMessageRefKey(guildId: GuildId, messageId: string) {
    return `guild:${guildId}:msg:${encodeURIComponent(messageId)}`;
}

function guildMessageRefPrefix(guildId: GuildId) {
    return `guild:${guildId}:msg:`;
}

function guildChannelMessageKey(guildId: GuildId, channelId: string, messageId: string) {
    return `guild:${guildId}:chanmsg:${encodeURIComponent(channelId)}:${encodeURIComponent(messageId)}`;
}

function guildChannelMessagePrefix(guildId: GuildId, channelId: string) {
    return `guild:${guildId}:chanmsg:${encodeURIComponent(channelId)}:`;
}

function isNotFoundError(error: any) {
    return error?.code === "LEVEL_NOT_FOUND" ||
        error?.code === "KEY_NOT_FOUND" ||
        error?.notFound ||
        (error?.message && error.message.includes("NotFound"));
}

export class LevelStore implements Store {
    private db: Level<string, string>;

    constructor(path: string) {
        this.db = new Level(path);
    }

    async append(guildId: GuildId, event: GuildEvent) {
        const value = JSON.stringify(event);
        const batch = this.db.batch();
        batch.put(guildSeqKey(guildId, event.seq), value);
        batch.put(`guild:${guildId}:head`, event.seq.toString());
        batch.put(`guild-index:${guildId}`, "1");
        for (const channelId of eventChannelIds(event)) {
            batch.put(guildChannelSeqKey(guildId, channelId, event.seq), value);
        }
        await this.writeDerivedIndexes(batch as any, guildId, event);
        await batch.write();
    }

    async getLog(guildId: GuildId): Promise<GuildEvent[]> {
        const events: GuildEvent[] = [];
        const iterator = this.db.iterator({
            gte: `guild:${guildId}:seq:${MIN_SEQ_KEY}`,
            lte: `guild:${guildId}:seq:${MAX_SEQ_KEY}`
        });

        for await (const [, value] of iterator) {
            events.push(JSON.parse(value));
        }

        return events;
    }

    async getHistory(query: HistoryQuery): Promise<GuildEvent[]> {
        if (query.channelId && !query.includeStructural) {
            return await this.getChannelHistory(query);
        }
        return selectHistoryEvents(await this.getLog(query.guildId), query);
    }

    async getSearchEvents(query: SearchEventsQuery): Promise<GuildEvent[]> {
        if (query.channelId && !query.includeStructural) {
            return await this.getChannelSearchEvents(query);
        }
        return await this.getGuildSearchEvents(query);
    }

    async getReplaySnapshotEvents(query: ReplaySnapshotQuery): Promise<GuildEvent[]> {
        const limit = this.normalizeReplaySnapshotLimit(query.limit);
        const events: GuildEvent[] = [];
        const iterator = this.db.iterator({
            gte: guildSeqKey(query.guildId, 0),
            lte: `guild:${query.guildId}:seq:${MAX_SEQ_KEY}`,
            reverse: true,
            limit
        });

        for await (const [, value] of iterator) {
            const event = JSON.parse(value) as GuildEvent;
            events.push(event);
            if (event.body.type === "CHECKPOINT") {
                break;
            }
        }

        events.reverse();
        return events;
    }

    async getMembersPage(query: MemberPageQuery): Promise<MemberPage> {
        const limit = normalizeMemberPageLimit(query.limit);
        const prefix = guildMemberPrefix(query.guildId);
        const iteratorOptions: any = query.afterUserId
            ? { gt: guildMemberKey(query.guildId, query.afterUserId), lt: `${prefix}~` }
            : { gte: prefix, lt: `${prefix}~` };
        iteratorOptions.limit = limit + 1;

        const members: SerializableMember[] = [];
        const iterator = this.db.iterator(iteratorOptions);
        for await (const [, value] of iterator) {
            members.push(JSON.parse(value));
        }

        const hasMore = members.length > limit;
        const visible = hasMore ? members.slice(0, limit) : members;
        return {
            members: visible,
            nextCursor: hasMore && visible.length > 0 ? visible[visible.length - 1].userId : null,
            hasMore
        };
    }

    async getMember(query: MemberQuery): Promise<SerializableMember | null> {
        return await this.readMember(query.guildId, query.userId) ?? null;
    }

    async getMessageRef(query: MessageRefQuery): Promise<SerializableMessageRef | null> {
        try {
            const value = await this.db.get(guildMessageRefKey(query.guildId, query.messageId));
            if (value === undefined) return null;
            return JSON.parse(value);
        } catch (e: any) {
            if (isNotFoundError(e)) return null;
            throw e;
        }
    }

    async getLastEvent(guildId: GuildId): Promise<GuildEvent | undefined> {
        try {
            const headSeqStr = await this.db.get(`guild:${guildId}:head`);
            const headSeq = parseInt(headSeqStr);
            const key = guildSeqKey(guildId, headSeq);
            const val = await this.db.get(key);

            if (!val) return undefined;
            return JSON.parse(val);
        } catch (e: any) {
            if (isNotFoundError(e)) return undefined;
            throw e;
        }
    }

    async deleteEvent(guildId: GuildId, seq: number) {
        const key = guildSeqKey(guildId, seq);
        const batch = this.db.batch();
        try {
            const value = await this.db.get(key);
            const event = JSON.parse(value) as GuildEvent;
            for (const channelId of eventChannelIds(event)) {
                batch.del(guildChannelSeqKey(guildId, channelId, seq));
            }
        } catch (e: any) {
            if (!isNotFoundError(e)) {
                throw e;
            }
        }
        batch.del(key);
        await batch.write();
        await this.rebuildGuildDerivedIndexes(guildId);
    }

    async getGuildIds(): Promise<GuildId[]> {
        const guilds = new Set<GuildId>();
        const iterator = this.db.keys({
            gte: "guild-index:",
            lte: "guild-index:~"
        });

        for await (const key of iterator) {
            guilds.add(key.slice("guild-index:".length));
        }

        if (guilds.size > 0) {
            return Array.from(guilds);
        }

        const legacyIterator = this.db.keys({
            gte: "guild:",
            lte: "guild:~"
        });

        for await (const key of legacyIterator) {
            const parts = key.split(":");
            if (parts.length >= 2 && parts[0] === "guild") guilds.add(parts[1]);
        }
        return Array.from(guilds);
    }

    async close() {
        await this.db.close();
    }

    private async getChannelHistory(query: HistoryQuery): Promise<GuildEvent[]> {
        const limit = Math.min(500, Math.max(1, Math.floor(Number(query.limit) || 100)));
        const prefix = guildChannelSeqPrefix(query.guildId, query.channelId!);
        const events: GuildEvent[] = [];

        if (query.afterSeq !== undefined) {
            const iterator = this.db.iterator({
                gt: `${prefix}${seqKey(query.afterSeq)}`,
                lte: `${prefix}${MAX_SEQ_KEY}`,
                limit
            });
            for await (const [, value] of iterator) {
                events.push(JSON.parse(value));
            }
            return events;
        }

        const iterator = this.db.iterator({
            gte: `${prefix}${MIN_SEQ_KEY}`,
            lt: query.beforeSeq !== undefined ? `${prefix}${seqKey(query.beforeSeq)}` : `${prefix}${MAX_SEQ_KEY}~`,
            reverse: true,
            limit
        });
        for await (const [, value] of iterator) {
            events.push(JSON.parse(value));
        }
        events.reverse();
        return events;
    }

    private async getGuildSearchEvents(query: SearchEventsQuery): Promise<GuildEvent[]> {
        const limit = this.normalizeSearchScanLimit(query.scanLimit);
        const events: GuildEvent[] = [];

        if (query.afterSeq !== undefined) {
            const iterator = this.db.iterator({
                gt: guildSeqKey(query.guildId, query.afterSeq),
                lte: `guild:${query.guildId}:seq:${MAX_SEQ_KEY}`,
                limit
            });
            for await (const [, value] of iterator) {
                events.push(JSON.parse(value));
            }
            return events;
        }

        const iterator = this.db.iterator({
            gte: guildSeqKey(query.guildId, 0),
            lt: query.beforeSeq !== undefined ? guildSeqKey(query.guildId, query.beforeSeq) : `guild:${query.guildId}:seq:~`,
            reverse: true,
            limit
        });
        for await (const [, value] of iterator) {
            events.push(JSON.parse(value));
        }
        events.reverse();
        return events;
    }

    private async getChannelSearchEvents(query: SearchEventsQuery): Promise<GuildEvent[]> {
        const limit = this.normalizeSearchScanLimit(query.scanLimit);
        const prefix = guildChannelSeqPrefix(query.guildId, query.channelId!);
        const events: GuildEvent[] = [];

        if (query.afterSeq !== undefined) {
            const iterator = this.db.iterator({
                gt: `${prefix}${seqKey(query.afterSeq)}`,
                lte: `${prefix}${MAX_SEQ_KEY}`,
                limit
            });
            for await (const [, value] of iterator) {
                events.push(JSON.parse(value));
            }
            return events;
        }

        const iterator = this.db.iterator({
            gte: `${prefix}${MIN_SEQ_KEY}`,
            lt: query.beforeSeq !== undefined ? `${prefix}${seqKey(query.beforeSeq)}` : `${prefix}${MAX_SEQ_KEY}~`,
            reverse: true,
            limit
        });
        for await (const [, value] of iterator) {
            events.push(JSON.parse(value));
        }
        events.reverse();
        return events;
    }

    private normalizeSearchScanLimit(limit: unknown) {
        const parsed = Number(limit);
        if (!Number.isFinite(parsed) || parsed <= 0) {
            return 5000;
        }
        return Math.min(100000, Math.floor(parsed));
    }

    private normalizeReplaySnapshotLimit(limit: unknown) {
        const parsed = Number(limit);
        if (!Number.isFinite(parsed) || parsed <= 0) {
            return 5000;
        }
        return Math.min(100000, Math.floor(parsed));
    }

    private async readMember(guildId: GuildId, userId: string): Promise<SerializableMember | undefined> {
        try {
            const value = await this.db.get(guildMemberKey(guildId, userId));
            if (value === undefined) return undefined;
            return JSON.parse(value);
        } catch (e: any) {
            if (isNotFoundError(e)) return undefined;
            throw e;
        }
    }

    private async getMessageRefValue(guildId: GuildId, messageId: string): Promise<SerializableMessageRef | undefined> {
        try {
            const value = await this.db.get(guildMessageRefKey(guildId, messageId));
            if (value === undefined) return undefined;
            return JSON.parse(value);
        } catch (e: any) {
            if (isNotFoundError(e)) return undefined;
            throw e;
        }
    }

    private async writeDerivedIndexes(batch: any, guildId: GuildId, event: GuildEvent) {
        const body = event.body as Record<string, any>;
        switch (body.type) {
            case "GUILD_CREATE":
                batch.put(guildMemberKey(guildId, event.author), JSON.stringify({
                    userId: event.author,
                    roles: ["owner"],
                    joinedAt: event.createdAt
                }));
                break;
            case "MEMBER_UPDATE": {
                const userId = typeof body.userId === "string" && body.userId.trim() ? body.userId : event.author;
                const current = await this.readMember(guildId, userId) || { userId, roles: [], joinedAt: event.createdAt };
                batch.put(guildMemberKey(guildId, userId), JSON.stringify({
                    ...current,
                    nickname: typeof body.nickname === "string" ? body.nickname : current.nickname,
                    avatar: typeof body.avatar === "string" ? body.avatar : current.avatar,
                    banner: typeof body.banner === "string" ? body.banner : current.banner,
                    bio: typeof body.bio === "string" ? body.bio : current.bio
                }));
                break;
            }
            case "ROLE_ASSIGN": {
                if (typeof body.userId !== "string" || typeof body.roleId !== "string") break;
                const current = await this.readMember(guildId, body.userId) || { userId: body.userId, roles: [], joinedAt: event.createdAt };
                batch.put(guildMemberKey(guildId, body.userId), JSON.stringify({
                    ...current,
                    roles: Array.from(new Set([...current.roles, body.roleId])).sort()
                }));
                break;
            }
            case "ROLE_REVOKE": {
                if (typeof body.userId !== "string" || typeof body.roleId !== "string") break;
                const current = await this.readMember(guildId, body.userId);
                if (current) {
                    batch.put(guildMemberKey(guildId, body.userId), JSON.stringify({
                        ...current,
                        roles: current.roles.filter((roleId) => roleId !== body.roleId)
                    }));
                }
                break;
            }
            case "ROLE_DELETE": {
                if (typeof body.roleId !== "string") break;
                const members = await this.readAllMembers(guildId);
                for (const member of members) {
                    if (member.roles.includes(body.roleId)) {
                        batch.put(guildMemberKey(guildId, member.userId), JSON.stringify({
                            ...member,
                            roles: member.roles.filter((roleId) => roleId !== body.roleId)
                        }));
                    }
                }
                break;
            }
            case "BAN_USER":
            case "BAN_ADD":
            case "MEMBER_KICK":
                if (typeof body.userId === "string") {
                    batch.del(guildMemberKey(guildId, body.userId));
                }
                break;
            case "MESSAGE": {
                if (typeof body.channelId !== "string") break;
                const messageId = typeof body.messageId === "string" && body.messageId.trim() ? body.messageId : event.id;
                const ref: SerializableMessageRef = {
                    channelId: body.channelId,
                    authorId: event.author
                };
                batch.put(guildMessageRefKey(guildId, messageId), JSON.stringify(ref));
                batch.put(guildChannelMessageKey(guildId, body.channelId, messageId), "1");
                break;
            }
            case "REACTION_ADD":
                await this.writeReaction(batch, guildId, body.messageId, body.reaction, event.author, true);
                break;
            case "REACTION_REMOVE": {
                const userId = typeof body.userId === "string" && body.userId.trim() ? body.userId : event.author;
                await this.writeReaction(batch, guildId, body.messageId, body.reaction, userId, false);
                break;
            }
            case "DELETE_MESSAGE": {
                if (typeof body.messageId !== "string") break;
                const ref = await this.getMessageRefValue(guildId, body.messageId);
                if (ref) {
                    batch.put(guildMessageRefKey(guildId, body.messageId), JSON.stringify({ ...ref, deleted: true }));
                }
                break;
            }
            case "CHANNEL_DELETE": {
                if (typeof body.channelId !== "string") break;
                const prefix = guildChannelMessagePrefix(guildId, body.channelId);
                const iterator = this.db.keys({ gte: prefix, lt: `${prefix}~` });
                for await (const key of iterator) {
                    const messageId = decodeURIComponent(key.slice(prefix.length));
                    const ref = await this.getMessageRefValue(guildId, messageId);
                    if (ref) {
                        batch.put(guildMessageRefKey(guildId, messageId), JSON.stringify({ ...ref, deleted: true }));
                    }
                }
                break;
            }
        }
    }

    private async writeReaction(batch: any, guildId: GuildId, messageId: unknown, reaction: unknown, userId: string, add: boolean) {
        if (typeof messageId !== "string" || typeof reaction !== "string" || !reaction.trim()) {
            return;
        }
        const ref = await this.getMessageRefValue(guildId, messageId);
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
        batch.put(guildMessageRefKey(guildId, messageId), JSON.stringify({
            ...ref,
            reactions: Object.keys(reactions).length > 0 ? reactions : undefined
        }));
    }

    private async readAllMembers(guildId: GuildId): Promise<SerializableMember[]> {
        const members: SerializableMember[] = [];
        const prefix = guildMemberPrefix(guildId);
        const iterator = this.db.iterator({ gte: prefix, lt: `${prefix}~` });
        for await (const [, value] of iterator) {
            members.push(JSON.parse(value));
        }
        return members;
    }

    private async rebuildGuildDerivedIndexes(guildId: GuildId) {
        const events = await this.getLog(guildId);
        const batch = this.db.batch();
        await this.deletePrefix(batch as any, guildMemberPrefix(guildId));
        await this.deletePrefix(batch as any, guildMessageRefPrefix(guildId));
        await this.deletePrefix(batch as any, `guild:${guildId}:chanmsg:`);

        const members = new Map<string, SerializableMember>();
        const messages = new Map<string, SerializableMessageRef>();
        const channelMessages = new Map<string, Set<string>>();
        for (const event of events) {
            this.applyDerivedToMemory(event, members, messages, channelMessages);
        }
        for (const member of members.values()) {
            batch.put(guildMemberKey(guildId, member.userId), JSON.stringify(member));
        }
        for (const [messageId, ref] of messages) {
            batch.put(guildMessageRefKey(guildId, messageId), JSON.stringify(ref));
            const channelRefs = channelMessages.get(ref.channelId);
            if (channelRefs?.has(messageId)) {
                batch.put(guildChannelMessageKey(guildId, ref.channelId, messageId), "1");
            }
        }
        await batch.write();
    }

    private async deletePrefix(batch: any, prefix: string) {
        const iterator = this.db.keys({ gte: prefix, lt: `${prefix}~` });
        for await (const key of iterator) {
            batch.del(key);
        }
    }

    private applyDerivedToMemory(
        event: GuildEvent,
        members: Map<string, SerializableMember>,
        messages: Map<string, SerializableMessageRef>,
        channelMessages: Map<string, Set<string>>
    ) {
        const body = event.body as Record<string, any>;
        switch (body.type) {
            case "GUILD_CREATE":
                members.set(event.author, { userId: event.author, roles: ["owner"], joinedAt: event.createdAt });
                break;
            case "MEMBER_UPDATE": {
                const userId = typeof body.userId === "string" && body.userId.trim() ? body.userId : event.author;
                const current = members.get(userId) || { userId, roles: [], joinedAt: event.createdAt };
                members.set(userId, {
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
                members.set(body.userId, { ...current, roles: Array.from(new Set([...current.roles, body.roleId])).sort() });
                break;
            }
            case "ROLE_REVOKE": {
                const current = typeof body.userId === "string" ? members.get(body.userId) : undefined;
                if (current && typeof body.roleId === "string") {
                    members.set(body.userId, { ...current, roles: current.roles.filter((roleId) => roleId !== body.roleId) });
                }
                break;
            }
            case "ROLE_DELETE":
                if (typeof body.roleId === "string") {
                    for (const [userId, member] of members) {
                        if (member.roles.includes(body.roleId)) {
                            members.set(userId, { ...member, roles: member.roles.filter((roleId) => roleId !== body.roleId) });
                        }
                    }
                }
                break;
            case "BAN_USER":
            case "BAN_ADD":
            case "MEMBER_KICK":
                if (typeof body.userId === "string") members.delete(body.userId);
                break;
            case "MESSAGE": {
                if (typeof body.channelId !== "string") break;
                const messageId = typeof body.messageId === "string" && body.messageId.trim() ? body.messageId : event.id;
                messages.set(messageId, { channelId: body.channelId, authorId: event.author });
                const refs = channelMessages.get(body.channelId) || new Set<string>();
                refs.add(messageId);
                channelMessages.set(body.channelId, refs);
                break;
            }
            case "REACTION_ADD":
            case "REACTION_REMOVE": {
                if (typeof body.messageId !== "string" || typeof body.reaction !== "string" || !body.reaction.trim()) break;
                const ref = messages.get(body.messageId);
                if (!ref) break;
                const reactions = { ...(ref.reactions ?? {}) };
                const users = new Set(reactions[body.reaction] ?? []);
                const userId = body.type === "REACTION_REMOVE" && typeof body.userId === "string" && body.userId.trim()
                    ? body.userId
                    : event.author;
                if (body.type === "REACTION_ADD") {
                    users.add(userId);
                } else {
                    users.delete(userId);
                }
                if (users.size > 0) {
                    reactions[body.reaction] = Array.from(users).sort();
                } else {
                    delete reactions[body.reaction];
                }
                messages.set(body.messageId, {
                    ...ref,
                    reactions: Object.keys(reactions).length > 0 ? reactions : undefined
                });
                break;
            }
            case "DELETE_MESSAGE": {
                const ref = typeof body.messageId === "string" ? messages.get(body.messageId) : undefined;
                if (ref) messages.set(body.messageId, { ...ref, deleted: true });
                break;
            }
            case "CHANNEL_DELETE": {
                if (typeof body.channelId !== "string") break;
                for (const messageId of channelMessages.get(body.channelId) || []) {
                    const ref = messages.get(messageId);
                    if (ref) messages.set(messageId, { ...ref, deleted: true });
                }
                break;
            }
        }
    }
}
