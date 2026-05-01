import { describe, expect, it } from "vitest";
import {
  applyEvent,
  createInitialState,
  GuildEvent,
  GuildState,
  validateEvent,
} from "@cgp/core";

const guildId = "moderation-permissions-guild";
const owner = "owner";
const moderator = "moderator";
const kicker = "kicker";
const banner = "banner";
const roleManager = "role-manager";
const member = "member";
const equalMember = "equal-member";
const highMember = "high-member";

function event(
  seq: number,
  body: Record<string, unknown>,
  author = owner,
): GuildEvent {
  return {
    id: `event-${seq}`,
    seq,
    prevHash: seq === 0 ? null : `event-${seq - 1}`,
    createdAt: 1700000000000 + seq,
    author,
    body: body as any,
    signature: `signature-${seq}`,
  };
}

function apply(
  state: GuildState,
  seq: number,
  body: Record<string, unknown>,
  author = owner,
) {
  return applyEvent(state, event(seq, body, author));
}

function buildState() {
  let seq = 0;
  let state = createInitialState(
    event(seq++, {
      type: "GUILD_CREATE",
      guildId,
      name: "Moderation Permissions",
    }),
  );
  const roles = [
    { roleId: "member-role", name: "Member", permissions: [], position: 1 },
    {
      roleId: "moderator-role",
      name: "Moderator",
      permissions: ["moderateMembers"],
      position: 10,
    },
    {
      roleId: "kicker-role",
      name: "Kicker",
      permissions: ["kickMembers"],
      position: 10,
    },
    {
      roleId: "banner-role",
      name: "Banner",
      permissions: ["banMembers"],
      position: 10,
    },
    {
      roleId: "role-manager-role",
      name: "Role Manager",
      permissions: ["manageRoles"],
      position: 30,
    },
    { roleId: "equal-role", name: "Equal", permissions: [], position: 10 },
    { roleId: "high-role", name: "High", permissions: [], position: 40 },
  ];
  for (const role of roles) {
    state = apply(state, seq++, { type: "ROLE_UPSERT", guildId, ...role });
  }
  const assignments = [
    [moderator, "moderator-role"],
    [kicker, "kicker-role"],
    [banner, "banner-role"],
    [roleManager, "role-manager-role"],
    [member, "member-role"],
    [equalMember, "equal-role"],
    [highMember, "high-role"],
  ];
  for (const [userId, roleId] of assignments) {
    state = apply(state, seq++, {
      type: "ROLE_ASSIGN",
      guildId,
      userId,
      roleId,
    });
  }
  return state;
}

describe("Svelte-compatible moderation validation", () => {
  it("requires exact kick, ban, and timeout permissions", () => {
    const state = buildState();

    expect(() =>
      validateEvent(
        state,
        event(100, { type: "MEMBER_KICK", guildId, userId: member }, kicker),
      ),
    ).not.toThrow();
    expect(() =>
      validateEvent(
        state,
        event(101, { type: "BAN_ADD", guildId, userId: member }, kicker),
      ),
    ).toThrow(/permission/);
    expect(() =>
      validateEvent(
        state,
        event(
          102,
          {
            type: "MEMBER_UPDATE",
            guildId,
            userId: member,
            timedOutUntil: new Date(Date.now() + 3600000).toISOString(),
          },
          kicker,
        ),
      ),
    ).toThrow(/permission/);

    expect(() =>
      validateEvent(
        state,
        event(103, { type: "BAN_ADD", guildId, userId: member }, banner),
      ),
    ).not.toThrow();
    expect(() =>
      validateEvent(
        state,
        event(104, { type: "MEMBER_KICK", guildId, userId: member }, banner),
      ),
    ).toThrow(/permission/);

    expect(() =>
      validateEvent(
        state,
        event(
          105,
          {
            type: "MEMBER_UPDATE",
            guildId,
            userId: member,
            timedOutUntil: new Date(Date.now() + 3600000).toISOString(),
          },
          moderator,
        ),
      ),
    ).not.toThrow();
    expect(() =>
      validateEvent(
        state,
        event(106, { type: "BAN_ADD", guildId, userId: member }, moderator),
      ),
    ).toThrow(/permission/);
  });

  it("enforces role hierarchy for member moderation", () => {
    const state = buildState();

    expect(() =>
      validateEvent(
        state,
        event(
          110,
          {
            type: "MEMBER_UPDATE",
            guildId,
            userId: equalMember,
            timedOutUntil: new Date(Date.now() + 3600000).toISOString(),
          },
          moderator,
        ),
      ),
    ).toThrow(/permission/);
    expect(() =>
      validateEvent(
        state,
        event(
          111,
          { type: "MEMBER_KICK", guildId, userId: highMember },
          kicker,
        ),
      ),
    ).toThrow(/permission/);
    expect(() =>
      validateEvent(
        state,
        event(112, { type: "BAN_ADD", guildId, userId: owner }, banner),
      ),
    ).toThrow(/permission/);
    expect(() =>
      validateEvent(
        state,
        event(113, { type: "BAN_ADD", guildId, userId: highMember }, owner),
      ),
    ).not.toThrow();
  });

  it("uses manageRoles and role hierarchy for role edits and member role updates", () => {
    const state = buildState();

    expect(() =>
      validateEvent(
        state,
        event(
          120,
          {
            type: "MEMBER_UPDATE",
            guildId,
            userId: member,
            roleIds: ["member-role"],
          },
          roleManager,
        ),
      ),
    ).not.toThrow();
    expect(() =>
      validateEvent(
        state,
        event(
          121,
          {
            type: "MEMBER_UPDATE",
            guildId,
            userId: member,
            roleIds: ["high-role"],
          },
          roleManager,
        ),
      ),
    ).toThrow(/permission/);
    expect(() =>
      validateEvent(
        state,
        event(
          122,
          {
            type: "MEMBER_UPDATE",
            guildId,
            userId: highMember,
            roleIds: ["member-role"],
          },
          roleManager,
        ),
      ),
    ).toThrow(/permission/);

    expect(() =>
      validateEvent(
        state,
        event(
          123,
          {
            type: "ROLE_UPSERT",
            guildId,
            roleId: "new-low-role",
            name: "New Low",
            permissions: [],
            position: 2,
          },
          roleManager,
        ),
      ),
    ).not.toThrow();
    expect(() =>
      validateEvent(
        state,
        event(
          124,
          {
            type: "ROLE_UPSERT",
            guildId,
            roleId: "new-high-role",
            name: "New High",
            permissions: [],
            position: 35,
          },
          roleManager,
        ),
      ),
    ).toThrow(/permission/);
    expect(() =>
      validateEvent(
        state,
        event(
          125,
          { type: "ROLE_DELETE", guildId, roleId: "high-role" },
          roleManager,
        ),
      ),
    ).toThrow(/permission/);
  });

  it("still allows members to update their own profile fields", () => {
    const state = buildState();

    expect(() =>
      validateEvent(
        state,
        event(
          130,
          { type: "MEMBER_UPDATE", guildId, nickname: "Local Nick" },
          member,
        ),
      ),
    ).not.toThrow();
    expect(() =>
      validateEvent(
        state,
        event(
          131,
          {
            type: "MEMBER_UPDATE",
            guildId,
            timedOutUntil: new Date(Date.now() + 3600000).toISOString(),
          },
          member,
        ),
      ),
    ).toThrow(/permission/);
  });
});
