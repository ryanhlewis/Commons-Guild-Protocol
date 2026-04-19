import { ChannelId, GuildEvent, GuildId } from "@cgp/core";

export interface HistoryQuery {
    guildId: GuildId;
    channelId?: ChannelId;
    beforeSeq?: number;
    afterSeq?: number;
    limit?: number;
    includeStructural?: boolean;
}

const DEFAULT_HISTORY_LIMIT = 100;
const MAX_HISTORY_LIMIT = 500;

function normalizeHistoryLimit(limit: unknown) {
    const parsed = Number(limit);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return DEFAULT_HISTORY_LIMIT;
    }
    return Math.min(MAX_HISTORY_LIMIT, Math.floor(parsed));
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
    const matching = log.filter((event) => matchesHistoryQuery(event, query));

    if (query.afterSeq !== undefined) {
        return matching.slice(0, limit);
    }

    return matching.slice(Math.max(0, matching.length - limit));
}

export interface Store {
    getLog(guildId: GuildId): Promise<GuildEvent[]> | GuildEvent[];
    getHistory?(query: HistoryQuery): Promise<GuildEvent[]> | GuildEvent[];
    append(guildId: GuildId, event: GuildEvent): Promise<void> | void;
    getLastEvent(guildId: GuildId): Promise<GuildEvent | undefined> | GuildEvent | undefined;
    getGuildIds(): Promise<GuildId[]> | GuildId[];
    deleteEvent(guildId: GuildId, seq: number): Promise<void> | void;
    close(): Promise<void> | void;
}

export class MemoryStore implements Store {
    private logs = new Map<GuildId, GuildEvent[]>();

    getLog(guildId: GuildId): GuildEvent[] {
        return this.logs.get(guildId) || [];
    }

    getHistory(query: HistoryQuery): GuildEvent[] {
        return selectHistoryEvents(this.getLog(query.guildId), query);
    }

    append(guildId: GuildId, event: GuildEvent) {
        const log = this.logs.get(guildId) || [];
        log.push(event);
        this.logs.set(guildId, log);
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
        }
    }

    close() {
        // No-op
    }
}
