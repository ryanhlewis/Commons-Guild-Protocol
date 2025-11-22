import { Level } from "level";
import { GuildEvent, GuildId } from "@cgp/core";

export class LevelStore {
    private db: Level<string, string>;

    constructor(path: string) {
        this.db = new Level(path);
    }

    async append(guildId: GuildId, event: GuildEvent) {
        // Key format: "guild:<guildId>:seq:<seq>" -> JSON(event)
        // Also keep "guild:<guildId>:head" -> seq

        const key = `guild:${guildId}:seq:${event.seq.toString().padStart(10, '0')}`;
        await this.db.put(key, JSON.stringify(event));
        await this.db.put(`guild:${guildId}:head`, event.seq.toString());
    }

    async getLog(guildId: GuildId): Promise<GuildEvent[]> {
        // Scan from seq 0 to head
        // Optimize: use iterator
        const events: GuildEvent[] = [];
        const iterator = this.db.iterator({
            gte: `guild:${guildId}:seq:0000000000`,
            lte: `guild:${guildId}:seq:9999999999`
        });

        for await (const [key, value] of iterator) {
            events.push(JSON.parse(value));
        }

        return events;
    }

    async getLastEvent(guildId: GuildId): Promise<GuildEvent | undefined> {
        try {
            const headSeqStr = await this.db.get(`guild:${guildId}:head`);

            const headSeq = parseInt(headSeqStr);
            const key = `guild:${guildId}:seq:${headSeq.toString().padStart(10, '0')}`;
            const val = await this.db.get(key);

            if (!val) return undefined;
            return JSON.parse(val);
        } catch (e: any) {

            if (e.code === 'LEVEL_NOT_FOUND' || e.code === 'KEY_NOT_FOUND' || e.notFound || (e.message && e.message.includes("NotFound"))) return undefined;
            throw e;
        }
    }

    async deleteEvent(guildId: GuildId, seq: number) {
        const key = `guild:${guildId}:seq:${seq.toString().padStart(10, '0')}`;
        await this.db.del(key);
    }

    async getGuildIds(): Promise<GuildId[]> {
        const guilds = new Set<GuildId>();
        // Scan all keys starting with "guild:"
        // This is inefficient for large DBs but fine for reference impl.
        // A better way would be to maintain a "guilds" index.
        const iterator = this.db.keys({
            gte: "guild:",
            lte: "guild:~"
        });

        for await (const key of iterator) {
            const parts = key.split(":");
            if (parts.length >= 2 && parts[0] === "guild") {
                guilds.add(parts[1]);
            }
        }
        return Array.from(guilds);
    }

    async close() {
        await this.db.close();
    }
}
