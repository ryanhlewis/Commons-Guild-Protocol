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
    AppObjectStateRef,
    SerializableGuildState,
    Checkpoint
} from "./types";
import { hashObject } from "./crypto";

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
    appObjects: Map<string, AppObjectStateRef>;
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
        appObjects: new Map(),
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
        appObjects: Array.from(state.appObjects.entries()),
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
        appObjects: new Map(serialized.appObjects ?? []),
        headSeq,
        headHash,
        createdAt,
        access: serialized.access,
        policies: serialized.policies || {}
    };
}

export interface RebuiltGuildState {
    state: GuildState;
    startIndex: number;
    checkpointEvent?: GuildEvent;
}

export function checkpointStateRoot(state: SerializableGuildState): HashHex {
    return hashObject(state);
}

export function deserializeCheckpointState(event: GuildEvent): GuildState {
    const body = event.body as Checkpoint;
    if (body.type !== "CHECKPOINT") {
        throw new Error("Event is not a checkpoint");
    }
    if (body.guildId !== event.body.guildId) {
        throw new Error("Checkpoint guildId mismatch");
    }
    if (body.state.guildId !== body.guildId) {
        throw new Error("Checkpoint state guildId mismatch");
    }
    if (typeof body.seq === "number" && body.seq !== event.seq) {
        throw new Error("Checkpoint body seq must match event seq");
    }
    if (checkpointStateRoot(body.state) !== body.rootHash) {
        throw new Error("Checkpoint rootHash does not match state");
    }
    return deserializeState(body.state, event.seq, event.id, event.createdAt);
}

export function isValidCheckpointEvent(event: GuildEvent): boolean {
    try {
        deserializeCheckpointState(event);
        return true;
    } catch {
        return false;
    }
}

export function rebuildStateFromEvents(events: GuildEvent[]): RebuiltGuildState {
    if (events.length === 0) {
        throw new Error("Cannot rebuild state from an empty event list");
    }

    let checkpointIndex = -1;
    let state: GuildState | undefined;
    for (let index = events.length - 1; index >= 0; index--) {
        if (events[index].body.type !== "CHECKPOINT") {
            continue;
        }
        try {
            state = deserializeCheckpointState(events[index]);
            checkpointIndex = index;
            break;
        } catch {
            // Ignore malformed checkpoints and fall back to an earlier anchor or genesis.
        }
    }

    const startIndex = checkpointIndex >= 0 ? checkpointIndex + 1 : 1;
    if (!state) {
        state = createInitialState(events[0]);
    }

    for (let i = startIndex; i < events.length; i++) {
        state = applyEvent(state, events[i], { mutable: true });
    }

    return {
        state,
        startIndex,
        checkpointEvent: checkpointIndex >= 0 ? events[checkpointIndex] : undefined
    };
}

export function applyEvent(state: GuildState, event: GuildEvent, options: { mutable?: boolean } = {}): GuildState {
    // Basic validation: seq must be state.headSeq + 1 (unless it's the first event, handled by createInitialState)
    // But here we assume we are applying valid events in order.

    const mutable = options.mutable === true;
    const newState = mutable ? state : { ...state };
    const ensureChannels = () => {
        if (mutable) return newState.channels;
        if (newState.channels === state.channels) {
            newState.channels = new Map(state.channels);
        }
        return newState.channels;
    };
    const ensureRoles = () => {
        if (mutable) return newState.roles;
        if (newState.roles === state.roles) {
            newState.roles = new Map(state.roles);
        }
        return newState.roles;
    };
    const ensureMembers = () => {
        if (mutable) return newState.members;
        if (newState.members === state.members) {
            newState.members = new Map(state.members);
        }
        return newState.members;
    };
    const ensureBans = () => {
        if (mutable) return newState.bans;
        if (newState.bans === state.bans) {
            newState.bans = new Map(state.bans);
        }
        return newState.bans;
    };
    const ensureMessages = () => {
        if (mutable) return newState.messages;
        if (newState.messages === state.messages) {
            newState.messages = new Map(state.messages);
        }
        return newState.messages;
    };
    const ensureAppObjects = () => {
        if (mutable) return newState.appObjects;
        if (newState.appObjects === state.appObjects) {
            newState.appObjects = new Map(state.appObjects);
        }
        return newState.appObjects;
    };
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
            ensureChannels().set(b.channelId, {
                id: b.channelId,
                name: b.name,
                kind: b.kind,
                retention: b.retention,
                categoryId: b.categoryId,
                description: b.description,
                topic: b.topic,
                position: b.position,
                permissionOverwrites: Array.isArray(b.permissionOverwrites)
                    ? b.permissionOverwrites
                    : undefined
            });
            break;
        }
        case "CHANNEL_UPSERT": {
            if (typeof bodyRecord.channelId === "string" && bodyRecord.channelId.trim()) {
                const current = state.channels.get(bodyRecord.channelId);
                ensureChannels().set(bodyRecord.channelId, {
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
                ensureChannels().delete(bodyRecord.channelId);
                for (const [messageId, message] of state.messages) {
                    if (message.channelId === bodyRecord.channelId) {
                        ensureMessages().set(messageId, {
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
                const current = state.roles.get(bodyRecord.roleId);
                ensureRoles().set(bodyRecord.roleId, {
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
                ensureRoles().delete(bodyRecord.roleId);
                for (const [userId, member] of state.members) {
                    if (member.roles.has(bodyRecord.roleId)) {
                        ensureMembers().set(userId, {
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
            const current = state.members.get(b.userId);
            const member = current
                ? { ...current, roles: new Set(current.roles) }
                : { userId: b.userId, roles: new Set<string>(), joinedAt: event.createdAt };
            member.roles.add(b.roleId);
            ensureMembers().set(b.userId, member);
            break;
        }
        case "ROLE_REVOKE": {
            const b = body as RoleRevoke;
            const current = state.members.get(b.userId);
            if (current) {
                const member = { ...current, roles: new Set(current.roles) };
                member.roles.delete(b.roleId);
                ensureMembers().set(b.userId, member);
            }
            break;
        }
        case "BAN_USER": {
            const b = body as BanUser;
            ensureBans().set(b.userId, {
                userId: b.userId,
                reason: b.reason,
                bannedAt: event.createdAt
            });
            ensureMembers().delete(b.userId);
            break;
        }
        case "BAN_ADD": {
            ensureBans().set(bodyRecord.userId, {
                userId: bodyRecord.userId,
                reason: bodyRecord.reason,
                expiresAt: bodyRecord.expiresAt,
                bannedAt: event.createdAt
            });
            ensureMembers().delete(bodyRecord.userId);
            break;
        }
        case "UNBAN_USER": {
            const b = body as UnbanUser;
            ensureBans().delete(b.userId);
            break;
        }
        case "BAN_REMOVE": {
            ensureBans().delete(bodyRecord.userId);
            break;
        }
        case "MEMBER_KICK": {
            ensureMembers().delete(bodyRecord.userId);
            break;
        }
        case "MESSAGE": {
            const b = body as Message;
            ensureMessages().set(b.messageId || event.id, {
                channelId: b.channelId,
                authorId: event.author,
                eventId: event.id,
                seq: event.seq
            });
            break;
        }
        case "REACTION_ADD": {
            const reaction = typeof bodyRecord.reaction === "string" ? bodyRecord.reaction.trim() : "";
            const message = state.messages.get(bodyRecord.messageId);
            if (reaction && message) {
                const reactions = { ...(message.reactions ?? {}) };
                const users = new Set(reactions[reaction] ?? []);
                users.add(event.author);
                reactions[reaction] = Array.from(users).sort();
                ensureMessages().set(bodyRecord.messageId, {
                    ...message,
                    reactions
                });
            }
            break;
        }
        case "REACTION_REMOVE": {
            const reaction = typeof bodyRecord.reaction === "string" ? bodyRecord.reaction.trim() : "";
            const message = state.messages.get(bodyRecord.messageId);
            if (reaction && message?.reactions?.[reaction]) {
                const reactions = { ...message.reactions };
                const removeUserId = typeof bodyRecord.userId === "string" && bodyRecord.userId.trim()
                    ? bodyRecord.userId
                    : event.author;
                const users = reactions[reaction].filter((userId) => userId !== removeUserId);
                if (users.length > 0) {
                    reactions[reaction] = users;
                } else {
                    delete reactions[reaction];
                }
                ensureMessages().set(bodyRecord.messageId, {
                    ...message,
                    reactions: Object.keys(reactions).length > 0 ? reactions : undefined
                });
            }
            break;
        }
        case "DELETE_MESSAGE": {
            const b = body as DeleteMessage;
            const message = state.messages.get(b.messageId);
            if (message) {
                ensureMessages().set(b.messageId, {
                    ...message,
                    deleted: true
                });
            }
            break;
        }
        case "APP_OBJECT_UPSERT": {
            if (typeof bodyRecord.namespace === "string" && typeof bodyRecord.objectType === "string" && typeof bodyRecord.objectId === "string") {
                const target = bodyRecord.target && typeof bodyRecord.target === "object" ? bodyRecord.target : undefined;
                const key = appObjectStateKey(bodyRecord.namespace, bodyRecord.objectType, bodyRecord.objectId);
                ensureAppObjects().set(key, {
                    namespace: bodyRecord.namespace,
                    objectType: bodyRecord.objectType,
                    objectId: bodyRecord.objectId,
                    channelId: bodyRecord.channelId ?? target?.channelId,
                    target,
                    value: bodyRecord.value,
                    authorId: event.author,
                    updatedAt: event.createdAt
                });
            }
            break;
        }
        case "APP_OBJECT_DELETE": {
            if (typeof bodyRecord.namespace === "string" && typeof bodyRecord.objectType === "string" && typeof bodyRecord.objectId === "string") {
                ensureAppObjects().delete(appObjectStateKey(bodyRecord.namespace, bodyRecord.objectType, bodyRecord.objectId));
            }
            break;
        }
        case "EPHEMERAL_POLICY_UPDATE": {
            const b = body as EphemeralPolicyUpdate;
            const channel = state.channels.get(b.channelId);
            if (channel) {
                ensureChannels().set(b.channelId, {
                    ...channel,
                    retention: b.retention
                });
            }
            break;
        }
        case "MEMBER_UPDATE": {
            const b = body as MemberUpdate;
            const targetUserId = bodyRecord.userId || event.author;
            const current = state.members.get(targetUserId);
            const member: Member = current
                ? { ...current, roles: new Set(current.roles) }
                : { userId: targetUserId, roles: new Set(), joinedAt: event.createdAt };
            if (b.nickname !== undefined) member.nickname = b.nickname;
            if (b.avatar !== undefined) member.avatar = b.avatar;
            if (b.banner !== undefined) member.banner = b.banner;
            if (b.bio !== undefined) member.bio = b.bio;
            if (Array.isArray(bodyRecord.roleIds)) member.roles = new Set(bodyRecord.roleIds.filter((roleId: unknown) => typeof roleId === "string"));
            if (bodyRecord.timedOutUntil !== undefined) (member as any).timedOutUntil = bodyRecord.timedOutUntil;
            if (bodyRecord.isMuted !== undefined) (member as any).isMuted = Boolean(bodyRecord.isMuted);
            if (bodyRecord.isDeafened !== undefined) (member as any).isDeafened = Boolean(bodyRecord.isDeafened);
            ensureMembers().set(targetUserId, member);
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

function appObjectStateKey(namespace: string, objectType: string, objectId: string) {
    return `${namespace}\u0000${objectType}\u0000${objectId}`;
}
