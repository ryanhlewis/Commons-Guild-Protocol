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
    private seqIndex = new Map<GuildId, Map<number, number>>(); // guildId -> seq -> array index

    getLog(guildId: GuildId): GuildEvent[] {
        return this.logs.get(guildId) || [];
    }

    append(guildId: GuildId, event: GuildEvent) {
        const log = this.logs.get(guildId) || [];
        const index = log.length;
        log.push(event);
        this.logs.set(guildId, log);
        
        // Update index
        let guildIndex = this.seqIndex.get(guildId);
        if (!guildIndex) {
            guildIndex = new Map();
            this.seqIndex.set(guildId, guildIndex);
        }
        guildIndex.set(event.seq, index);
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
        
        // Use index for O(1) lookup
        const guildIndex = this.seqIndex.get(guildId);
        if (!guildIndex) return;
        
        const index = guildIndex.get(seq);
        if (index !== undefined && index < log.length && log[index]?.seq === seq) {
            log.splice(index, 1);
            // Rebuild index for this guild after deletion
            this.rebuildGuildIndex(guildId, log);
        } else {
            // Fallback to linear search if index is stale
            const fallbackIndex = log.findIndex(e => e.seq === seq);
            if (fallbackIndex !== -1) {
                log.splice(fallbackIndex, 1);
                this.rebuildGuildIndex(guildId, log);
            }
        }
    }

    private rebuildGuildIndex(guildId: GuildId, log: GuildEvent[]) {
        const guildIndex = new Map<number, number>();
        for (let i = 0; i < log.length; i++) {
            guildIndex.set(log[i].seq, i);
        }
        this.seqIndex.set(guildId, guildIndex);
    }

    close() {
        // No-op
    }
}
