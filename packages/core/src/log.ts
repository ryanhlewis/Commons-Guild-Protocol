import { hashObject } from "./crypto";
import { GuildEvent, HashHex } from "./types";

export function computeEventId(unsigned: Omit<GuildEvent, "id" | "signature">): HashHex {
    return hashObject(unsigned);
}

export function validateChain(events: GuildEvent[]): boolean {
    for (let i = 0; i < events.length; i++) {
        const ev = events[i];
        if (ev.seq !== i) return false;
        if (i === 0 && ev.prevHash !== null) return false;
        if (i > 0 && ev.prevHash !== events[i - 1].id) return false;

        const expectedId = computeEventId({
            seq: ev.seq,
            prevHash: ev.prevHash,
            createdAt: ev.createdAt,
            author: ev.author,
            body: ev.body,
        });

        if (expectedId !== ev.id) return false;

        // Stateful checks such as duplicate channel IDs require replay state and live in validateEvent().
        // Note: Signature verification is separate, usually done before chain validation or as part of it if we pass in verify fn
    }
    return true;
}
