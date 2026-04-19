import {
    GuildEvent,
    GuildId,
    ChannelId,
    HashHex,
    UserId,
    EventBody,
    GuildCreate,
    ChannelCreate,
    RoleAssign,
    RoleRevoke,
    BanUser,
    UnbanUser,
    Message,
    DeleteMessage,
    EphemeralPolicy,
    EphemeralPolicyUpdate,
    MemberUpdate,
    Channel,
    Role,
    Member,
    Ban,
    GuildPolicies,
    SerializableMessageRef,
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
    messages: Map<HashHex, SerializableMessageRef>;
    createdAt: number;
    headSeq: number;
    headHash: string;
    access: "public" | "private";
    policies: GuildPolicies;
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
        messages: new Map(),
        createdAt: event.createdAt,
        headSeq: event.seq,
        headHash: event.id,
        access: body.access || "public",
        policies: body.policies || {}
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
        messages: Array.from(state.messages.entries()),
        access: state.access,
        policies: state.policies
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
        messages: new Map(serialized.messages ?? []),
        headSeq,
        headHash,
        createdAt,
        access: serialized.access,
        policies: serialized.policies || {}
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
    newState.messages = new Map(state.messages);
    newState.headSeq = event.seq;
    newState.headHash = event.id;
    newState.access = state.access;
    newState.policies = state.policies;

    const body = event.body;
    const bodyRecord = body as any;

    switch (bodyRecord.type) {
        case "GUILD_UPDATE": {
            if (typeof bodyRecord.name === "string" && bodyRecord.name.trim()) {
                newState.name = bodyRecord.name;
            }
            if (typeof bodyRecord.description === "string") {
                newState.description = bodyRecord.description;
            }
            if (bodyRecord.access === "public" || bodyRecord.access === "private") {
                newState.access = bodyRecord.access;
            }
            if (bodyRecord.policies && typeof bodyRecord.policies === "object") {
                newState.policies = {
                    ...newState.policies,
                    ...bodyRecord.policies
                };
            }
            break;
        }
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
        case "CHANNEL_UPSERT": {
            if (typeof bodyRecord.channelId === "string" && bodyRecord.channelId.trim()) {
                const current = newState.channels.get(bodyRecord.channelId);
                newState.channels.set(bodyRecord.channelId, {
                    ...(current ?? {
                        id: bodyRecord.channelId,
                        name: bodyRecord.channelId,
                        kind: "text"
                    }),
                    id: bodyRecord.channelId,
                    name: typeof bodyRecord.name === "string" && bodyRecord.name.trim() ? bodyRecord.name : current?.name ?? bodyRecord.channelId,
                    kind: (bodyRecord.kind || current?.kind || "text") as any,
                    retention: bodyRecord.retention ?? current?.retention,
                    categoryId: bodyRecord.categoryId,
                    description: bodyRecord.description,
                    topic: bodyRecord.topic,
                    position: bodyRecord.position,
                    permissionOverwrites: Array.isArray(bodyRecord.permissionOverwrites)
                        ? bodyRecord.permissionOverwrites
                        : (current as any)?.permissionOverwrites
                } as any);
            }
            break;
        }
        case "CHANNEL_DELETE": {
            if (typeof bodyRecord.channelId === "string") {
                newState.channels.delete(bodyRecord.channelId);
                for (const [messageId, message] of newState.messages) {
                    if (message.channelId === bodyRecord.channelId) {
                        newState.messages.set(messageId, {
                            ...message,
                            deleted: true
                        });
                    }
                }
            }
            break;
        }
        case "ROLE_UPSERT": {
            if (typeof bodyRecord.roleId === "string" && bodyRecord.roleId.trim()) {
                const current = newState.roles.get(bodyRecord.roleId);
                newState.roles.set(bodyRecord.roleId, {
                    ...(current ?? {
                        id: bodyRecord.roleId,
                        name: bodyRecord.roleId,
                        permissions: []
                    }),
                    id: bodyRecord.roleId,
                    name: typeof bodyRecord.name === "string" && bodyRecord.name.trim() ? bodyRecord.name : current?.name ?? bodyRecord.roleId,
                    permissions: Array.isArray(bodyRecord.permissions)
                        ? bodyRecord.permissions.filter((permission: unknown) => typeof permission === "string")
                        : current?.permissions ?? [],
                    color: bodyRecord.color,
                    icon: bodyRecord.icon,
                    position: bodyRecord.position,
                    mentionable: bodyRecord.mentionable,
                    hoist: bodyRecord.hoist,
                    managed: bodyRecord.managed
                } as any);
            }
            break;
        }
        case "ROLE_DELETE": {
            if (typeof bodyRecord.roleId === "string") {
                newState.roles.delete(bodyRecord.roleId);
                for (const [userId, member] of newState.members) {
                    if (member.roles.has(bodyRecord.roleId)) {
                        newState.members.set(userId, {
                            ...member,
                            roles: new Set([...member.roles].filter((roleId) => roleId !== bodyRecord.roleId))
                        });
                    }
                }
            }
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
        case "BAN_ADD": {
            newState.bans.set(bodyRecord.userId, {
                userId: bodyRecord.userId,
                reason: bodyRecord.reason,
                expiresAt: bodyRecord.expiresAt,
                bannedAt: event.createdAt
            });
            newState.members.delete(bodyRecord.userId);
            break;
        }
        case "UNBAN_USER": {
            const b = body as UnbanUser;
            newState.bans.delete(b.userId);
            break;
        }
        case "BAN_REMOVE": {
            newState.bans.delete(bodyRecord.userId);
            break;
        }
        case "MEMBER_KICK": {
            newState.members.delete(bodyRecord.userId);
            break;
        }
        case "MESSAGE": {
            const b = body as Message;
            newState.messages.set(b.messageId || event.id, {
                channelId: b.channelId,
                authorId: event.author
            });
            break;
        }
        case "DELETE_MESSAGE": {
            const b = body as DeleteMessage;
            const message = newState.messages.get(b.messageId);
            if (message) {
                newState.messages.set(b.messageId, {
                    ...message,
                    deleted: true
                });
            }
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
        case "MEMBER_UPDATE": {
            const b = body as MemberUpdate;
            const targetUserId = bodyRecord.userId || event.author;
            const member: Member = newState.members.get(targetUserId) || { userId: targetUserId, roles: new Set(), joinedAt: event.createdAt };
            if (b.nickname !== undefined) member.nickname = b.nickname;
            if (b.avatar !== undefined) member.avatar = b.avatar;
            if (b.banner !== undefined) member.banner = b.banner;
            if (b.bio !== undefined) member.bio = b.bio;
            if (Array.isArray(bodyRecord.roleIds)) member.roles = new Set(bodyRecord.roleIds.filter((roleId: unknown) => typeof roleId === "string"));
            if (bodyRecord.timedOutUntil !== undefined) (member as any).timedOutUntil = bodyRecord.timedOutUntil;
            if (bodyRecord.isMuted !== undefined) (member as any).isMuted = Boolean(bodyRecord.isMuted);
            if (bodyRecord.isDeafened !== undefined) (member as any).isDeafened = Boolean(bodyRecord.isDeafened);
            newState.members.set(targetUserId, member);
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
