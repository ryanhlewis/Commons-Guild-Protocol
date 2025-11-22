import { GuildEvent, GuildId } from "@cgp/core";

export interface Store {
    getLog(guildId: GuildId): Promise<GuildEvent[]> | GuildEvent[];
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
