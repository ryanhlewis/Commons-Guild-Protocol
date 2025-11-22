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
        headHash: event.id
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
        bans: Array.from(state.bans.entries())
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
        createdAt
    };
}

export function applyEvent(state: GuildState, event: GuildEvent): GuildState {
    // Basic validation: seq must be state.headSeq + 1 (unless it's the first event, handled by createInitialState)
    // But here we assume we are applying valid events in order.

    const newState = { ...state }; // Shallow copy
    // Deep copy maps/sets to avoid mutation issues if we were using immutable patterns strictly,
    // but for performance in this reference impl we might mutate if we are careful.
    // Let's do a mix: copy the containers we modify.
    newState.channels = new Map(state.channels);
    newState.roles = new Map(state.roles);
    newState.members = new Map(state.members);
    newState.bans = new Map(state.bans);
    newState.headSeq = event.seq;
    newState.headHash = event.id;

    const body = event.body;

    switch (body.type) {
        case "CHANNEL_CREATE": {
            const b = body as ChannelCreate;
            newState.channels.set(b.channelId, {
                id: b.channelId,
                name: b.name,
                kind: b.kind,
                retention: b.retention
            });
            break;
        }
        case "ROLE_ASSIGN": {
            const b = body as RoleAssign;
            const member = newState.members.get(b.userId) || { userId: b.userId, roles: new Set(), joinedAt: event.createdAt };
            // Copy roles set
            member.roles = new Set(member.roles);
            member.roles.add(b.roleId);
            newState.members.set(b.userId, member);
            break;
        }
        case "ROLE_REVOKE": {
            const b = body as RoleRevoke;
            const member = newState.members.get(b.userId);
            if (member) {
                member.roles = new Set(member.roles);
                member.roles.delete(b.roleId);
                newState.members.set(b.userId, member);
            }
            break;
        }
        case "BAN_USER": {
            const b = body as BanUser;
            newState.bans.set(b.userId, {
                userId: b.userId,
                reason: b.reason,
                bannedAt: event.createdAt
            });
            // Also remove from members?
            newState.members.delete(b.userId);
            break;
        }
        case "UNBAN_USER": {
            const b = body as UnbanUser;
            newState.bans.delete(b.userId);
            break;
        }
        case "EPHEMERAL_POLICY_UPDATE": {
            const b = body as EphemeralPolicyUpdate;
            const channel = newState.channels.get(b.channelId);
            if (channel) {
                newState.channels.set(b.channelId, {
                    ...channel,
                    retention: b.retention
                });
            }
            break;
        }
        case "CHECKPOINT": {
            const b = body as Checkpoint;
            // If we are applying a checkpoint event, it usually means we are verifying it or loading from it.
            // If we are blindly applying it, we might overwrite state.
            // However, in a linear log, a checkpoint is just a snapshot.
            // If we trust the source, we could replace the state.
            // But typically checkpoints are used to START state reconstruction, not applied in the middle.
            // If it appears in the middle, it's advisory/backup.
            // For now, let's just validate that it matches our current state if we wanted to be strict,
            // or just ignore it as it doesn't CHANGE state (it reflects it).
            // Let's ignore it for state transitions, as it doesn't mutate the guild.
            break;
        }
        // MESSAGE, EDIT_MESSAGE, DELETE_MESSAGE don't change guild structure state (usually).
        // They might update "last message" pointers if we tracked that.
        // For now, we ignore them in the structural state.
    }

    return newState;
}
