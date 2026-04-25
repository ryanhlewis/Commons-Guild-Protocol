import { Level } from "level";
import { GuildEvent, GuildId, SerializableMember, SerializableMessageRef } from "@cgp/core";
import {
    eventChannelIds,
    HistoryQuery,
    MemberQuery,
    MemberPage,
    MemberPageQuery,
    MemberSearchQuery,
    MessageRefQuery,
    MessageRefsQuery,
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

function seqFromPrefixedKey(key: string, prefix: string) {
    const seq = Number(key.slice(prefix.length));
    return Number.isInteger(seq) && seq >= 0 ? seq : null;
}

function eventFromChannelIndexValue(value: unknown): GuildEvent | undefined {
    if (typeof value !== "string" || value.length === 0 || value[0] !== "{") {
        return undefined;
    }

    try {
        return JSON.parse(value) as GuildEvent;
    } catch {
        return undefined;
    }
}

function guildMemberKey(guildId: GuildId, userId: string) {
    return `guild:${guildId}:member:${encodeURIComponent(userId)}`;
}

function guildMemberPrefix(guildId: GuildId) {
    return `guild:${guildId}:member:`;
}

function guildMemberSearchKey(guildId: GuildId, term: string, userId: string) {
    return `guild:${guildId}:memberSearch:${encodeURIComponent(term)}:${encodeURIComponent(userId)}`;
}

function guildMemberSearchPrefix(guildId: GuildId, term = "") {
    return `guild:${guildId}:memberSearch:${encodeURIComponent(term)}`;
}

function guildMemberSearchDocKey(guildId: GuildId, userId: string) {
    return `guild:${guildId}:memberSearchDoc:${encodeURIComponent(userId)}`;
}

function guildMemberSearchDocPrefix(guildId: GuildId) {
    return `guild:${guildId}:memberSearchDoc:`;
}

function guildRoleMemberKey(guildId: GuildId, roleId: string, userId: string) {
    return `guild:${guildId}:roleMember:${encodeURIComponent(roleId)}:${encodeURIComponent(userId)}`;
}

function guildRoleMemberPrefix(guildId: GuildId, roleId = "") {
    return `guild:${guildId}:roleMember:${encodeURIComponent(roleId)}`;
}

function guildMessageRefKey(guildId: GuildId, messageId: string) {
    return `guild:${guildId}:msg:${encodeURIComponent(messageId)}`;
}

function guildMessageRefPrefix(guildId: GuildId) {
    return `guild:${guildId}:msg:`;
}

function isNotFoundError(error: any) {
    return error?.code === "LEVEL_NOT_FOUND" ||
        error?.code === "KEY_NOT_FOUND" ||
        error?.notFound ||
        (error?.message && error.message.includes("NotFound"));
}

function memberSearchTokens(value: unknown) {
    if (typeof value !== "string") return [];
    return value
        .toLowerCase()
        .split(/[^a-z0-9_.-]+/i)
        .map((entry) => entry.trim())
        .filter((entry) => entry.length > 0 && entry.length <= 128);
}

function memberSearchTerms(member: SerializableMember) {
    const terms = new Set<string>();
    for (const value of [member.userId, member.nickname, ...(member.roles ?? [])]) {
        for (const token of memberSearchTokens(value)) {
            terms.add(token);
            if (terms.size >= 48) {
                return [...terms];
            }
        }
    }
    return [...terms];
}

function memberMatchesSearch(member: SerializableMember, tokens: string[]) {
    if (tokens.length === 0) return false;
    const haystack = [member.userId, member.nickname, member.bio, ...(member.roles ?? [])]
        .filter((value): value is string => typeof value === "string" && value.length > 0)
        .join(" ")
        .toLowerCase();
    return tokens.every((token) => haystack.includes(token));
}

function needsDerivedRebuildAfterBatch(events: GuildEvent[]) {
    if (events.length <= 1) {
        return false;
    }

    for (const event of events) {
        const body = event.body as Record<string, any>;
        switch (body.type) {
            case "CHANNEL_DELETE":
                return true;
        }
    }

    return false;
}

function batchMemberIndexUserId(event: GuildEvent): string | null {
    const body = event.body as Record<string, any>;
    switch (body.type) {
        case "MEMBER_UPDATE":
        case "BAN_USER":
        case "BAN_ADD":
        case "MEMBER_KICK":
            return typeof body.userId === "string" && body.userId.trim() ? body.userId : event.author;
        case "ROLE_ASSIGN":
        case "ROLE_REVOKE":
            return typeof body.userId === "string" && body.userId.trim() ? body.userId : null;
        default:
            return null;
    }
}

function batchMemberIndexUserIds(events: GuildEvent[]) {
    const userIds = new Set<string>();
    for (const event of events) {
        const userId = batchMemberIndexUserId(event);
        if (userId) {
            userIds.add(userId);
        }
    }
    return [...userIds];
}

function batchMessageRefIds(events: GuildEvent[]) {
    const messageIds = new Set<string>();
    for (const event of events) {
        const body = event.body as Record<string, any>;
        switch (body.type) {
            case "REACTION_ADD":
            case "REACTION_REMOVE":
            case "DELETE_MESSAGE":
                if (typeof body.messageId === "string" && body.messageId.trim()) {
                    messageIds.add(body.messageId);
                }
                break;
        }
    }
    return [...messageIds];
}

function isMessageAppendBatch(events: GuildEvent[]) {
    if (events.length === 0) {
        return false;
    }
    for (const event of events) {
        const body = event.body as Record<string, any>;
        if (body.type !== "MESSAGE" || typeof body.channelId !== "string" || !body.channelId.trim()) {
            return false;
        }
    }
    return true;
}

interface MemberIndexSnapshot {
    member?: SerializableMember;
    searchTerms: string[];
}

export class LevelStore implements Store {
    private db: Level<string, string>;

    constructor(path: string) {
        this.db = new Level(path);
    }

    async append(guildId: GuildId, event: GuildEvent) {
        await this.appendEvents(guildId, [event]);
    }

    async appendEvents(guildId: GuildId, events: GuildEvent[]) {
        if (events.length === 0) {
            return;
        }
        if (isMessageAppendBatch(events)) {
            await this.appendMessageEvents(guildId, events);
            return;
        }

        const batch = this.db.batch();
        const needsDerivedRebuild = needsDerivedRebuildAfterBatch(events);
        const memberSnapshots = !needsDerivedRebuild
            ? await this.readMemberIndexSnapshots(guildId, batchMemberIndexUserIds(events))
            : undefined;
        const messageSnapshots = !needsDerivedRebuild
            ? await this.readMessageRefSnapshots(guildId, batchMessageRefIds(events))
            : undefined;

        for (const event of events) {
            const value = JSON.stringify(event);
            batch.put(guildSeqKey(guildId, event.seq), value);
            for (const channelId of eventChannelIds(event)) {
                batch.put(guildChannelSeqKey(guildId, channelId, event.seq), value);
            }
            if (!needsDerivedRebuild) {
                await this.writeDerivedIndexes(batch as any, guildId, event, memberSnapshots, messageSnapshots);
            }
        }

        const lastEvent = events[events.length - 1];
        batch.put(`guild:${guildId}:head`, lastEvent.seq.toString());
        batch.put(`guild-index:${guildId}`, "1");
        await batch.write();

        if (needsDerivedRebuild) {
            await this.rebuildGuildDerivedIndexes(guildId);
        }
    }

    private async appendMessageEvents(guildId: GuildId, events: GuildEvent[]) {
        const batch = this.db.batch();
        for (const event of events) {
            const body = event.body as Record<string, any>;
            const value = JSON.stringify(event);
            const channelId = body.channelId as string;
            const messageId = typeof body.messageId === "string" && body.messageId.trim()
                ? body.messageId
                : event.id;
            const ref: SerializableMessageRef = {
                channelId,
                authorId: event.author,
                eventId: event.id,
                seq: event.seq
            };

            batch.put(guildSeqKey(guildId, event.seq), value);
            batch.put(guildChannelSeqKey(guildId, channelId, event.seq), value);
            batch.put(guildMessageRefKey(guildId, messageId), JSON.stringify(ref));
        }

        const lastEvent = events[events.length - 1];
        batch.put(`guild:${guildId}:head`, lastEvent.seq.toString());
        batch.put(`guild-index:${guildId}`, "1");
        await batch.write();
    }

    async *iterateLog(guildId: GuildId): AsyncIterable<GuildEvent> {
        const iterator = this.db.iterator({
            gte: `guild:${guildId}:seq:${MIN_SEQ_KEY}`,
            lte: `guild:${guildId}:seq:${MAX_SEQ_KEY}`
        });

        for await (const [, value] of iterator) {
            yield JSON.parse(value) as GuildEvent;
        }
    }

    async getLog(guildId: GuildId): Promise<GuildEvent[]> {
        const events: GuildEvent[] = [];
        for await (const event of this.iterateLog(guildId)) {
            events.push(event);
        }

        return events;
    }

    async getEventRange(query: { guildId: GuildId; afterSeq?: number; limit?: number }): Promise<GuildEvent[]> {
        const limit = Math.min(10000, Math.max(1, Math.floor(Number(query.limit) || 5000)));
        const lowerSeq = query.afterSeq === undefined ? 0 : query.afterSeq + 1;
        const events: GuildEvent[] = [];
        const iterator = this.db.iterator({
            gte: guildSeqKey(query.guildId, lowerSeq),
            lte: `guild:${query.guildId}:seq:${MAX_SEQ_KEY}`,
            limit
        });

        for await (const [, value] of iterator) {
            events.push(JSON.parse(value) as GuildEvent);
        }
        return events;
    }

    async getHistory(query: HistoryQuery): Promise<GuildEvent[]> {
        if (query.channelId && !query.includeStructural) {
            return await this.getChannelHistory(query);
        }
        if (query.channelId && query.includeStructural) {
            return await this.getChannelHistoryWithStructural(query);
        }
        return await this.getGuildHistory(query);
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

    async searchMembers(query: MemberSearchQuery): Promise<MemberPage> {
        const limit = normalizeMemberPageLimit(query.limit);
        const tokens = memberSearchTokens(query.query);
        const firstToken = tokens[0];
        if (!firstToken) {
            return { members: [], nextCursor: null, hasMore: false };
        }

        const members: SerializableMember[] = [];
        const seen = new Set<string>();
        const prefix = guildMemberSearchPrefix(query.guildId, firstToken);
        const iterator = this.db.iterator({ gte: prefix, lt: `${prefix}~`, limit: limit * 12 });
        for await (const [, value] of iterator) {
            const userId = typeof value === "string" ? value : "";
            if (!userId || seen.has(userId)) {
                continue;
            }
            seen.add(userId);
            const member = await this.readMember(query.guildId, userId);
            if (!member || !memberMatchesSearch(member, tokens)) {
                continue;
            }
            members.push(member);
            if (members.length >= limit + 1) {
                break;
            }
        }

        const hasMore = members.length > limit;
        const visible = hasMore ? members.slice(0, limit) : members;
        return {
            members: visible,
            nextCursor: null,
            hasMore
        };
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

    async getMessageRefs(query: MessageRefsQuery): Promise<Map<string, SerializableMessageRef>> {
        const refs = new Map<string, SerializableMessageRef>();
        const messageIds = Array.from(new Set(query.messageIds.filter((messageId) => typeof messageId === "string" && messageId.length > 0)));
        if (messageIds.length === 0) {
            return refs;
        }

        for (let offset = 0; offset < messageIds.length; offset += 1024) {
            const chunk = messageIds.slice(offset, offset + 1024);
            const values = await this.db.getMany(chunk.map((messageId) => guildMessageRefKey(query.guildId, messageId)));
            for (let index = 0; index < chunk.length; index += 1) {
                const value = values[index];
                if (typeof value === "string") {
                    refs.set(chunk[index], JSON.parse(value));
                }
            }
        }

        return refs;
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
        await this.deleteEvents(guildId, [seq]);
    }

    async deleteEvents(guildId: GuildId, seqs: number[]) {
        const uniqueSeqs = Array.from(new Set(seqs.filter((seq) => Number.isInteger(seq)))).sort((a, b) => a - b);
        if (uniqueSeqs.length === 0) {
            return;
        }

        const batch = this.db.batch();
        for (const seq of uniqueSeqs) {
            const key = guildSeqKey(guildId, seq);
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
        }
        await batch.write();
        await this.repairGuildHead(guildId);
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

    async repairIndexes(guildId?: GuildId) {
        const guildIds = guildId ? [guildId] : await this.getGuildIds();
        const results: Array<{ guildId: GuildId; events: number; headSeq: number; headHash: string | null }> = [];
        for (const id of guildIds) {
            await this.repairGuildHead(id);
            await this.rebuildGuildDerivedIndexes(id);
            const head = await this.getLastEvent(id);
            results.push({
                guildId: id,
                events: head ? head.seq + 1 : 0,
                headSeq: head?.seq ?? -1,
                headHash: head?.id ?? null
            });
        }
        return results;
    }

    async close() {
        await this.db.close();
    }

    async compact(query: { start?: string; end?: string } = {}) {
        const db = this.db as any;
        if (typeof db.open === "function" && db.status !== "open") {
            await db.open();
        }
        const start = query.start ?? "";
        const end = query.end ?? "\xff";
        if (typeof db.compactRange === "function") {
            await db.compactRange(start, end);
            return;
        }
        if (typeof db.db?.compactRange === "function") {
            await db.db.compactRange(start, end);
        }
    }

    private async repairGuildHead(guildId: GuildId) {
        const event = await this.readLastEventBySeq(guildId);
        if (event) {
            await this.db.put(`guild:${guildId}:head`, event.seq.toString());
            return;
        }
        await this.db.del(`guild:${guildId}:head`).catch((e: any) => {
            if (!isNotFoundError(e)) throw e;
        });
        await this.db.del(`guild-index:${guildId}`).catch((e: any) => {
            if (!isNotFoundError(e)) throw e;
        });
    }

    private async getFirstEvent(guildId: GuildId): Promise<GuildEvent | undefined> {
        const iterator = this.db.iterator({
            gte: guildSeqKey(guildId, 0),
            lte: `guild:${guildId}:seq:${MAX_SEQ_KEY}`,
            limit: 1
        });

        for await (const [, value] of iterator) {
            return JSON.parse(value) as GuildEvent;
        }
        return undefined;
    }

    private async readLastEventBySeq(guildId: GuildId): Promise<GuildEvent | undefined> {
        const iterator = this.db.iterator({
            gte: guildSeqKey(guildId, 0),
            lte: `guild:${guildId}:seq:${MAX_SEQ_KEY}`,
            reverse: true,
            limit: 1
        });

        for await (const [, value] of iterator) {
            return JSON.parse(value) as GuildEvent;
        }
        return undefined;
    }

    private async readEventsBySeq(guildId: GuildId, seqs: number[]) {
        const events: GuildEvent[] = [];
        for (let offset = 0; offset < seqs.length; offset += 512) {
            const chunk = seqs.slice(offset, offset + 512);
            const values = await this.db.getMany(chunk.map((seq) => guildSeqKey(guildId, seq)));
            for (const value of values) {
                if (typeof value === "string") {
                    events.push(JSON.parse(value) as GuildEvent);
                }
            }
        }
        return events;
    }

    private async readEventsFromChannelIndex(
        guildId: GuildId,
        prefix: string,
        iteratorOptions: Record<string, unknown>
    ) {
        const entries: Array<{ event?: GuildEvent; seq?: number }> = [];
        const fallbackSeqs: number[] = [];
        const iterator = this.db.iterator(iteratorOptions);
        for await (const [key, value] of iterator) {
            const event = eventFromChannelIndexValue(value);
            if (event) {
                entries.push({ event });
                continue;
            }

            const seq = seqFromPrefixedKey(key, prefix);
            if (seq !== null) {
                entries.push({ seq });
                fallbackSeqs.push(seq);
            }
        }

        if (fallbackSeqs.length === 0) {
            return entries.flatMap((entry) => entry.event ? [entry.event] : []);
        }

        const fallbackEvents = await this.readEventsBySeq(guildId, fallbackSeqs);
        const fallbackBySeq = new Map(fallbackEvents.map((event) => [event.seq, event]));
        return entries.flatMap((entry) => {
            if (entry.event) return [entry.event];
            const event = entry.seq !== undefined ? fallbackBySeq.get(entry.seq) : undefined;
            return event ? [event] : [];
        });
    }

    private async readChannelMessageIds(guildId: GuildId, channelId: string) {
        const prefix = guildChannelSeqPrefix(guildId, channelId);
        const messageIds: string[] = [];
        let seqs: number[] = [];

        const flush = async () => {
            if (seqs.length === 0) return;
            const events = await this.readEventsBySeq(guildId, seqs);
            for (const event of events) {
                const body = event.body as Record<string, any>;
                if (body.type !== "MESSAGE" || body.channelId !== channelId) {
                    continue;
                }
                messageIds.push(typeof body.messageId === "string" && body.messageId.trim() ? body.messageId : event.id);
            }
            seqs = [];
        };

        const iterator = this.db.iterator({ gte: `${prefix}${MIN_SEQ_KEY}`, lt: `${prefix}${MAX_SEQ_KEY}~` });
        for await (const [key, value] of iterator) {
            const indexedEvent = eventFromChannelIndexValue(value);
            if (indexedEvent) {
                const body = indexedEvent.body as Record<string, any>;
                if (body.type === "MESSAGE" && body.channelId === channelId) {
                    messageIds.push(typeof body.messageId === "string" && body.messageId.trim() ? body.messageId : indexedEvent.id);
                }
                continue;
            }

            const seq = seqFromPrefixedKey(key, prefix);
            if (seq === null) continue;
            seqs.push(seq);
            if (seqs.length >= 512) {
                await flush();
            }
        }
        await flush();
        return messageIds;
    }

    private async getChannelHistory(query: HistoryQuery): Promise<GuildEvent[]> {
        const limit = Math.min(500, Math.max(1, Math.floor(Number(query.limit) || 100)));
        const prefix = guildChannelSeqPrefix(query.guildId, query.channelId!);

        if (query.afterSeq !== undefined) {
            return await this.readEventsFromChannelIndex(query.guildId, prefix, {
                gt: `${prefix}${seqKey(query.afterSeq)}`,
                lte: `${prefix}${MAX_SEQ_KEY}`,
                limit
            });
        }

        const events = await this.readEventsFromChannelIndex(query.guildId, prefix, {
            gte: `${prefix}${MIN_SEQ_KEY}`,
            lt: query.beforeSeq !== undefined ? `${prefix}${seqKey(query.beforeSeq)}` : `${prefix}${MAX_SEQ_KEY}~`,
            reverse: true,
            limit
        });
        events.reverse();
        return events;
    }

    private async getChannelHistoryWithStructural(query: HistoryQuery): Promise<GuildEvent[]> {
        const limit = Math.min(500, Math.max(1, Math.floor(Number(query.limit) || 100)));
        const channelEvents = await this.getChannelHistory({ ...query, includeStructural: false, limit: limit + 1 });
        const firstEvent = await this.getFirstEvent(query.guildId);
        if (firstEvent?.body.type === "GUILD_CREATE") {
            channelEvents.push(firstEvent);
        }
        channelEvents.sort((left, right) => left.seq - right.seq);
        return selectHistoryEvents(channelEvents, query);
    }

    private async getGuildHistory(query: HistoryQuery): Promise<GuildEvent[]> {
        const limit = Math.min(500, Math.max(1, Math.floor(Number(query.limit) || 100)));
        const events: GuildEvent[] = [];

        if (query.afterSeq !== undefined) {
            const iterator = this.db.iterator({
                gt: guildSeqKey(query.guildId, query.afterSeq),
                lte: `guild:${query.guildId}:seq:${MAX_SEQ_KEY}`,
                limit
            });
            for await (const [, value] of iterator) {
                events.push(JSON.parse(value) as GuildEvent);
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
            events.push(JSON.parse(value) as GuildEvent);
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

        if (query.afterSeq !== undefined) {
            return await this.readEventsFromChannelIndex(query.guildId, prefix, {
                gt: `${prefix}${seqKey(query.afterSeq)}`,
                lte: `${prefix}${MAX_SEQ_KEY}`,
                limit
            });
        }

        const events = await this.readEventsFromChannelIndex(query.guildId, prefix, {
            gte: `${prefix}${MIN_SEQ_KEY}`,
            lt: query.beforeSeq !== undefined ? `${prefix}${seqKey(query.beforeSeq)}` : `${prefix}${MAX_SEQ_KEY}~`,
            reverse: true,
            limit
        });
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

    private async readMessageRefSnapshots(guildId: GuildId, messageIds: string[]) {
        const snapshots = new Map<string, SerializableMessageRef | undefined>();
        if (messageIds.length === 0) {
            return snapshots;
        }

        for (let offset = 0; offset < messageIds.length; offset += 1024) {
            const chunk = messageIds.slice(offset, offset + 1024);
            const values = await this.db.getMany(chunk.map((messageId) => guildMessageRefKey(guildId, messageId)));
            for (let index = 0; index < chunk.length; index += 1) {
                const value = values[index];
                snapshots.set(chunk[index], typeof value === "string" ? JSON.parse(value) : undefined);
            }
        }

        return snapshots;
    }

    private async getMessageRefForDerived(
        guildId: GuildId,
        messageId: string,
        messageSnapshots?: Map<string, SerializableMessageRef | undefined>
    ) {
        if (messageSnapshots?.has(messageId)) {
            return messageSnapshots.get(messageId);
        }

        const ref = await this.getMessageRefValue(guildId, messageId);
        messageSnapshots?.set(messageId, ref);
        return ref;
    }

    private putMessageRefForDerived(
        batch: any,
        guildId: GuildId,
        messageId: string,
        ref: SerializableMessageRef,
        messageSnapshots?: Map<string, SerializableMessageRef | undefined>
    ) {
        messageSnapshots?.set(messageId, ref);
        batch.put(guildMessageRefKey(guildId, messageId), JSON.stringify(ref));
    }

    private async readMemberSearchTerms(guildId: GuildId, userId: string): Promise<string[]> {
        try {
            const value = await this.db.get(guildMemberSearchDocKey(guildId, userId));
            if (value === undefined) return [];
            const parsed = JSON.parse(value);
            return Array.isArray(parsed) ? parsed.filter((entry): entry is string => typeof entry === "string") : [];
        } catch (e: any) {
            if (isNotFoundError(e)) return [];
            throw e;
        }
    }

    private async readMemberIndexSnapshots(guildId: GuildId, userIds: string[]) {
        const snapshots = new Map<string, MemberIndexSnapshot>();
        if (userIds.length === 0) {
            return snapshots;
        }

        for (let offset = 0; offset < userIds.length; offset += 1024) {
            const chunk = userIds.slice(offset, offset + 1024);
            const [memberValues, searchTermValues] = await Promise.all([
                this.db.getMany(chunk.map((userId) => guildMemberKey(guildId, userId))),
                this.db.getMany(chunk.map((userId) => guildMemberSearchDocKey(guildId, userId)))
            ]);

            for (let index = 0; index < chunk.length; index += 1) {
                const memberValue = memberValues[index];
                const searchTermValue = searchTermValues[index];
                let member: SerializableMember | undefined;
                let searchTerms: string[] = [];

                if (typeof memberValue === "string") {
                    member = JSON.parse(memberValue) as SerializableMember;
                }
                if (typeof searchTermValue === "string") {
                    const parsed = JSON.parse(searchTermValue);
                    searchTerms = Array.isArray(parsed)
                        ? parsed.filter((entry): entry is string => typeof entry === "string")
                        : [];
                }
                snapshots.set(chunk[index], { member, searchTerms });
            }
        }
        return snapshots;
    }

    private async deleteMemberSearchKeys(batch: any, guildId: GuildId, userId: string, previousTerms?: string[]) {
        const terms = previousTerms ?? await this.readMemberSearchTerms(guildId, userId);
        for (const term of terms) {
            batch.del(guildMemberSearchKey(guildId, term, userId));
        }
        batch.del(guildMemberSearchDocKey(guildId, userId));
    }

    private async putIndexedMember(
        batch: any,
        guildId: GuildId,
        member: SerializableMember,
        previousTerms?: string[],
        previousMember?: SerializableMember
    ) {
        await this.deleteMemberSearchKeys(batch, guildId, member.userId, previousTerms);
        batch.put(guildMemberKey(guildId, member.userId), JSON.stringify(member));
        const terms = memberSearchTerms(member);
        batch.put(guildMemberSearchDocKey(guildId, member.userId), JSON.stringify(terms));
        for (const term of terms) {
            batch.put(guildMemberSearchKey(guildId, term, member.userId), member.userId);
        }
        const previousRoles = new Set(previousMember?.roles ?? []);
        const nextRoles = new Set(member.roles ?? []);
        for (const roleId of previousRoles) {
            if (!nextRoles.has(roleId)) {
                batch.del(guildRoleMemberKey(guildId, roleId, member.userId));
            }
        }
        for (const roleId of nextRoles) {
            if (!previousRoles.has(roleId)) {
                batch.put(guildRoleMemberKey(guildId, roleId, member.userId), member.userId);
            }
        }
        return terms;
    }

    private async deleteIndexedMember(
        batch: any,
        guildId: GuildId,
        userId: string,
        previousTerms?: string[],
        previousMember?: SerializableMember
    ) {
        batch.del(guildMemberKey(guildId, userId));
        await this.deleteMemberSearchKeys(batch, guildId, userId, previousTerms);
        for (const roleId of previousMember?.roles ?? []) {
            batch.del(guildRoleMemberKey(guildId, roleId, userId));
        }
    }

    private async putIndexedMemberForDerived(
        batch: any,
        guildId: GuildId,
        member: SerializableMember,
        memberSnapshots?: Map<string, MemberIndexSnapshot>,
        previousTerms?: string[],
        previousMember?: SerializableMember
    ) {
        const terms = await this.putIndexedMember(batch, guildId, member, previousTerms, previousMember);
        memberSnapshots?.set(member.userId, { member, searchTerms: terms });
    }

    private async deleteIndexedMemberForDerived(
        batch: any,
        guildId: GuildId,
        userId: string,
        memberSnapshots?: Map<string, MemberIndexSnapshot>,
        previousTerms?: string[],
        previousMember?: SerializableMember
    ) {
        await this.deleteIndexedMember(batch, guildId, userId, previousTerms, previousMember);
        memberSnapshots?.set(userId, { member: undefined, searchTerms: [] });
    }

    private async writeDerivedIndexes(
        batch: any,
        guildId: GuildId,
        event: GuildEvent,
        memberSnapshots?: Map<string, MemberIndexSnapshot>,
        messageSnapshots?: Map<string, SerializableMessageRef | undefined>
    ) {
        const body = event.body as Record<string, any>;
        switch (body.type) {
            case "GUILD_CREATE": {
                await this.putIndexedMemberForDerived(batch, guildId, {
                    userId: event.author,
                    roles: ["owner"],
                    joinedAt: event.createdAt
                }, memberSnapshots);
                break;
            }
            case "MEMBER_UPDATE": {
                const userId = typeof body.userId === "string" && body.userId.trim() ? body.userId : event.author;
                const snapshot = memberSnapshots?.get(userId);
                const current = snapshot?.member || await this.readMember(guildId, userId) || { userId, roles: [], joinedAt: event.createdAt };
                await this.putIndexedMemberForDerived(batch, guildId, {
                    ...current,
                    nickname: typeof body.nickname === "string" ? body.nickname : current.nickname,
                    avatar: typeof body.avatar === "string" ? body.avatar : current.avatar,
                    banner: typeof body.banner === "string" ? body.banner : current.banner,
                    bio: typeof body.bio === "string" ? body.bio : current.bio
                }, memberSnapshots, snapshot?.searchTerms, snapshot?.member);
                break;
            }
            case "ROLE_ASSIGN": {
                if (typeof body.userId !== "string" || typeof body.roleId !== "string") break;
                const snapshot = memberSnapshots?.get(body.userId);
                const current = snapshot?.member || await this.readMember(guildId, body.userId) || { userId: body.userId, roles: [], joinedAt: event.createdAt };
                await this.putIndexedMemberForDerived(batch, guildId, {
                    ...current,
                    roles: Array.from(new Set([...current.roles, body.roleId])).sort()
                }, memberSnapshots, snapshot?.searchTerms, snapshot?.member);
                break;
            }
            case "ROLE_REVOKE": {
                if (typeof body.userId !== "string" || typeof body.roleId !== "string") break;
                const snapshot = memberSnapshots?.get(body.userId);
                const current = snapshot?.member || await this.readMember(guildId, body.userId);
                if (current) {
                    await this.putIndexedMemberForDerived(batch, guildId, {
                        ...current,
                        roles: current.roles.filter((roleId) => roleId !== body.roleId)
                    }, memberSnapshots, snapshot?.searchTerms, snapshot?.member);
                }
                break;
            }
            case "ROLE_DELETE": {
                if (typeof body.roleId !== "string") break;
                const members = await this.readRoleMembers(guildId, body.roleId);
                for (const member of members) {
                    if (member.roles.includes(body.roleId)) {
                        await this.putIndexedMember(batch, guildId, {
                            ...member,
                            roles: member.roles.filter((roleId) => roleId !== body.roleId)
                        }, undefined, member);
                    }
                }
                break;
            }
            case "BAN_USER":
            case "BAN_ADD":
            case "MEMBER_KICK":
                if (typeof body.userId === "string") {
                    const snapshot = memberSnapshots?.get(body.userId);
                    await this.deleteIndexedMemberForDerived(batch, guildId, body.userId, memberSnapshots, snapshot?.searchTerms, snapshot?.member);
                }
                break;
            case "MESSAGE": {
                if (typeof body.channelId !== "string") break;
                const messageId = typeof body.messageId === "string" && body.messageId.trim() ? body.messageId : event.id;
                const ref: SerializableMessageRef = {
                    channelId: body.channelId,
                    authorId: event.author,
                    eventId: event.id,
                    seq: event.seq
                };
                this.putMessageRefForDerived(batch, guildId, messageId, ref, messageSnapshots);
                break;
            }
            case "REACTION_ADD":
                await this.writeReaction(batch, guildId, body.messageId, body.reaction, event.author, true, messageSnapshots);
                break;
            case "REACTION_REMOVE": {
                const userId = typeof body.userId === "string" && body.userId.trim() ? body.userId : event.author;
                await this.writeReaction(batch, guildId, body.messageId, body.reaction, userId, false, messageSnapshots);
                break;
            }
            case "DELETE_MESSAGE": {
                if (typeof body.messageId !== "string") break;
                const ref = await this.getMessageRefForDerived(guildId, body.messageId, messageSnapshots);
                if (ref) {
                    this.putMessageRefForDerived(batch, guildId, body.messageId, { ...ref, deleted: true }, messageSnapshots);
                }
                break;
            }
            case "CHANNEL_DELETE": {
                if (typeof body.channelId !== "string") break;
                const messageIds = await this.readChannelMessageIds(guildId, body.channelId);
                for (const messageId of messageIds) {
                    const ref = await this.getMessageRefForDerived(guildId, messageId, messageSnapshots);
                    if (ref) {
                        this.putMessageRefForDerived(batch, guildId, messageId, { ...ref, deleted: true }, messageSnapshots);
                    }
                }
                break;
            }
        }
    }

    private async writeReaction(
        batch: any,
        guildId: GuildId,
        messageId: unknown,
        reaction: unknown,
        userId: string,
        add: boolean,
        messageSnapshots?: Map<string, SerializableMessageRef | undefined>
    ) {
        if (typeof messageId !== "string" || typeof reaction !== "string" || !reaction.trim()) {
            return;
        }
        const ref = await this.getMessageRefForDerived(guildId, messageId, messageSnapshots);
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
        this.putMessageRefForDerived(batch, guildId, messageId, {
            ...ref,
            reactions: Object.keys(reactions).length > 0 ? reactions : undefined
        }, messageSnapshots);
    }

    private async readRoleMembers(guildId: GuildId, roleId: string): Promise<SerializableMember[]> {
        const userIds: string[] = [];
        const prefix = `${guildRoleMemberPrefix(guildId, roleId)}:`;
        const iterator = this.db.keys({ gte: prefix, lt: `${prefix}~` });
        for await (const key of iterator) {
            userIds.push(decodeURIComponent(key.slice(prefix.length)));
        }

        const members: SerializableMember[] = [];
        for (let offset = 0; offset < userIds.length; offset += 1024) {
            const chunk = userIds.slice(offset, offset + 1024);
            const values = await this.db.getMany(chunk.map((userId) => guildMemberKey(guildId, userId)));
            for (const value of values) {
                if (typeof value === "string") {
                    members.push(JSON.parse(value));
                }
            }
        }
        return members;
    }

    private async rebuildGuildDerivedIndexes(guildId: GuildId) {
        const events = await this.getLog(guildId);
        const batch = this.db.batch();
        await this.deletePrefix(batch as any, guildMemberPrefix(guildId));
        await this.deletePrefix(batch as any, guildMemberSearchPrefix(guildId));
        await this.deletePrefix(batch as any, guildMemberSearchDocPrefix(guildId));
        await this.deletePrefix(batch as any, guildRoleMemberPrefix(guildId));
        await this.deletePrefix(batch as any, guildMessageRefPrefix(guildId));
        // Delete legacy channel-message reference indexes; the channel seq index is the canonical source now.
        await this.deletePrefix(batch as any, `guild:${guildId}:chanmsg:`);

        const members = new Map<string, SerializableMember>();
        const messages = new Map<string, SerializableMessageRef>();
        const channelMessages = new Map<string, Set<string>>();
        for (const event of events) {
            this.applyDerivedToMemory(event, members, messages, channelMessages);
        }
        for (const member of members.values()) {
            await this.putIndexedMember(batch, guildId, member);
        }
        for (const [messageId, ref] of messages) {
            batch.put(guildMessageRefKey(guildId, messageId), JSON.stringify(ref));
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
                messages.set(messageId, { channelId: body.channelId, authorId: event.author, eventId: event.id, seq: event.seq });
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
