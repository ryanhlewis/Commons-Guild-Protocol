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

        // Extended validation
        if (ev.body.type === "CHANNEL_CREATE") {
            // Check uniqueness if we had the full state, but here we just validate the chain structure.
            // We can enforce that channelId is the hash of the event?
            // The spec says "channel_id is its hash".
            // But channelId is inside the body.
            // So we can't easily validate it without re-hashing the body excluding channelId?
            // Or maybe channelId is NOT in the body for the hash?
            // For now, let's just ensure basic structure.
        }
        // Note: Signature verification is separate, usually done before chain validation or as part of it if we pass in verify fn
    }
    return true;
}
