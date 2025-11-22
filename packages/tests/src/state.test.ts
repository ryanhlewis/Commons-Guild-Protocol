import { describe, it, expect } from "vitest";
import { createInitialState, applyEvent, GuildState } from "@cgp/core/src/state";
import { GuildEvent, GuildCreate, ChannelCreate, RoleAssign, BanUser } from "@cgp/core/src/types";
import { computeEventId } from "@cgp/core/src/log";

function createMockEvent(seq: number, body: any, author: string = "author_pubkey"): GuildEvent {
    const ev: any = {
        seq,
        prevHash: seq === 0 ? null : "prev_hash",
        createdAt: Date.now(),
        author,
        body,
        signature: "sig"
    };
    ev.id = computeEventId(ev);
    return ev;
}

describe("State Reconstruction", () => {
    const guildId = "test_guild_id";
    const ownerId = "owner_pubkey";

    it("creates initial state from GUILD_CREATE", () => {
        const body: GuildCreate = {
            type: "GUILD_CREATE",
            guildId,
            name: "Test Guild",
            description: "A test guild"
        };
        const event = createMockEvent(0, body, ownerId);
        const state = createInitialState(event);

        expect(state.guildId).toBe(guildId);
        expect(state.name).toBe("Test Guild");
        expect(state.ownerId).toBe(ownerId);
        expect(state.members.has(ownerId)).toBe(true);
        expect(state.members.get(ownerId)?.roles.has("owner")).toBe(true);
    });

    it("applies CHANNEL_CREATE", () => {
        const body: GuildCreate = { type: "GUILD_CREATE", guildId, name: "Test Guild" };
        let state = createInitialState(createMockEvent(0, body, ownerId));

        const channelBody: ChannelCreate = {
            type: "CHANNEL_CREATE",
            guildId,
            channelId: "chan_1",
            name: "general",
            kind: "text"
        };
        state = applyEvent(state, createMockEvent(1, channelBody, ownerId));

        expect(state.channels.size).toBe(1);
        expect(state.channels.get("chan_1")?.name).toBe("general");
    });

    it("applies ROLE_ASSIGN and ROLE_REVOKE", () => {
        const body: GuildCreate = { type: "GUILD_CREATE", guildId, name: "Test Guild" };
        let state = createInitialState(createMockEvent(0, body, ownerId));

        const userId = "user_1";
        const roleId = "moderator";

        // Assign
        const assignBody: RoleAssign = {
            type: "ROLE_ASSIGN",
            guildId,
            userId,
            roleId
        };
        state = applyEvent(state, createMockEvent(1, assignBody, ownerId));

        expect(state.members.has(userId)).toBe(true);
        expect(state.members.get(userId)?.roles.has(roleId)).toBe(true);

        // Revoke
        const revokeBody: any = { // Using any to avoid strict type check if interface missing in test context? No, should be fine.
            type: "ROLE_REVOKE",
            guildId,
            userId,
            roleId
        };
        state = applyEvent(state, createMockEvent(2, revokeBody, ownerId));

        expect(state.members.get(userId)?.roles.has(roleId)).toBe(false);
    });

    it("applies BAN_USER", () => {
        const body: GuildCreate = { type: "GUILD_CREATE", guildId, name: "Test Guild" };
        let state = createInitialState(createMockEvent(0, body, ownerId));

        const userId = "bad_user";
        // Add user first (implicitly via role assign or just existence? logic adds member on role assign)
        // But ban should work even if not member?

        const banBody: BanUser = {
            type: "BAN_USER",
            guildId,
            userId,
            reason: "spam"
        };
        state = applyEvent(state, createMockEvent(1, banBody, ownerId));

        expect(state.bans.has(userId)).toBe(true);
        expect(state.members.has(userId)).toBe(false);
    });
});
