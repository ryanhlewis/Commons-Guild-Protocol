import {
    GuildEvent,
    GuildId,
    ChannelId,
    UserId,
    EventBody,
    GuildCreate,
    ChannelCreate,
    RoleAssign,
    RoleRevoke,
    BanUser,
    UnbanUser,
    EphemeralPolicy,
    EphemeralPolicyUpdate,
    MemberUpdate,
    Channel,
    Role,
    Member,
    Ban,
    SerializableGuildState,
    Checkpoint
} from "./types";

export interface GuildState {
    guildId: GuildId;
    name: string;
    description?: string;
    ownerId: UserId;
    channels: Map<ChannelId, Channel>;
    roles: Map<string, Role>; // roleId -> Role
    members: Map<UserId, Member>;
    bans: Map<UserId, Ban>;
    createdAt: number;
    headSeq: number;
    headHash: string;
    access: "public" | "private";
}

export function createInitialState(event: GuildEvent): GuildState {
    if (event.body.type !== "GUILD_CREATE") {
        throw new Error("First event must be GUILD_CREATE");
    }
    const body = event.body as GuildCreate;
    return {
        guildId: body.guildId,
        name: body.name,
        description: body.description,
        ownerId: event.author,
        channels: new Map(),
        roles: new Map(),
        members: new Map([[event.author, { userId: event.author, roles: new Set(["owner"]), joinedAt: event.createdAt }]]),
        bans: new Map(),
        createdAt: event.createdAt,
        headSeq: event.seq,
        headHash: event.id,
        access: body.access || "public"
    };
}

export function serializeState(state: GuildState): SerializableGuildState {
    return {
        guildId: state.guildId,
        name: state.name,
        description: state.description || "",
        ownerId: state.ownerId,
        channels: Array.from(state.channels.entries()),
        members: Array.from(state.members.entries()).map(([id, member]) => [
            id,
            {
                ...member,
                roles: Array.from(member.roles)
            }
        ]),
        roles: Array.from(state.roles.entries()),
        bans: Array.from(state.bans.entries()),
        access: state.access
    };
}

export function deserializeState(serialized: SerializableGuildState, headSeq: number, headHash: string, createdAt: number): GuildState {
    const members = new Map<UserId, Member>();
    for (const [id, sMember] of serialized.members) {
        members.set(id, {
            ...sMember,
            roles: new Set(sMember.roles)
        });
    }

    return {
        guildId: serialized.guildId,
        name: serialized.name,
        description: serialized.description,
        ownerId: serialized.ownerId,
        channels: new Map(serialized.channels),
        members,
        roles: new Map(serialized.roles),
        bans: new Map(serialized.bans),
        headSeq,
        headHash,
        createdAt,
        access: serialized.access
    };
}

export function applyEvent(state: GuildState, event: GuildEvent): GuildState {
    // Basic validation: seq must be state.headSeq + 1 (unless it's the first event, handled by createInitialState)
    // But here we assume we are applying valid events in order.

    const body = event.body;

    // Optimization: Only copy Maps that will actually be modified
    let newChannels = state.channels;
    let newRoles = state.roles;
    let newMembers = state.members;
    let newBans = state.bans;

    switch (body.type) {
        case "CHANNEL_CREATE": {
            const b = body as ChannelCreate;
            newChannels = new Map(state.channels);
            newChannels.set(b.channelId, {
                id: b.channelId,
                name: b.name,
                kind: b.kind,
                retention: b.retention
            });
            break;
        }
        case "ROLE_ASSIGN": {
            const b = body as RoleAssign;
            newMembers = new Map(state.members);
            const member = newMembers.get(b.userId) || { userId: b.userId, roles: new Set(), joinedAt: event.createdAt };
            // Copy roles set
            const newRolesSet = new Set(member.roles);
            newRolesSet.add(b.roleId);
            newMembers.set(b.userId, { ...member, roles: newRolesSet });
            break;
        }
        case "ROLE_REVOKE": {
            const b = body as RoleRevoke;
            const member = state.members.get(b.userId);
            if (member) {
                newMembers = new Map(state.members);
                const newRolesSet = new Set(member.roles);
                newRolesSet.delete(b.roleId);
                newMembers.set(b.userId, { ...member, roles: newRolesSet });
            }
            break;
        }
        case "BAN_USER": {
            const b = body as BanUser;
            newBans = new Map(state.bans);
            newBans.set(b.userId, {
                userId: b.userId,
                reason: b.reason,
                bannedAt: event.createdAt
            });
            // Also remove from members?
            if (state.members.has(b.userId)) {
                newMembers = new Map(state.members);
                newMembers.delete(b.userId);
            }
            break;
        }
        case "UNBAN_USER": {
            const b = body as UnbanUser;
            newBans = new Map(state.bans);
            newBans.delete(b.userId);
            break;
        }
        case "EPHEMERAL_POLICY_UPDATE": {
            const b = body as EphemeralPolicyUpdate;
            const channel = state.channels.get(b.channelId);
            if (channel) {
                newChannels = new Map(state.channels);
                newChannels.set(b.channelId, {
                    ...channel,
                    retention: b.retention
                });
            }
            break;
        }
        case "MEMBER_UPDATE": {
            const b = body as MemberUpdate;
            const member = newState.members.get(event.author) || { userId: event.author, roles: new Set(), joinedAt: event.createdAt };
            if (b.nickname !== undefined) member.nickname = b.nickname;
            if (b.avatar !== undefined) member.avatar = b.avatar;
            newState.members.set(event.author, member);
            break;
        }
        case "CHECKPOINT": {
            // Checkpoints don't change guild structure state
            break;
        }
        // MESSAGE, EDIT_MESSAGE, DELETE_MESSAGE don't change guild structure state (usually).
        // They might update "last message" pointers if we tracked that.
        // For now, we ignore them in the structural state.
    }

    // Only create a new state object if something actually changed
    // Note: headSeq and headHash always change, so we focus on collection changes
    const collectionsChanged = newChannels !== state.channels || newRoles !== state.roles || 
        newMembers !== state.members || newBans !== state.bans;

    return {
        ...state,
        channels: newChannels,
        roles: newRoles,
        members: newMembers,
        bans: newBans,
        headSeq: event.seq,
        headHash: event.id
    };
}
