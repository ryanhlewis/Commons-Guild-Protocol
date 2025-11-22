import { GuildEvent, GuildId } from "@cgp/core";

export class MemoryStore {
    private logs = new Map<GuildId, GuildEvent[]>();

    getLog(guildId: GuildId): GuildEvent[] {
        return this.logs.get(guildId) || [];
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
}
