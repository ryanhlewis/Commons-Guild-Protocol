import { AppObjectDelete, AppObjectUpsert, ChannelCreate, DeleteMessage, EditMessage, GuildEvent, Message } from "./types";
import { GuildState } from "./state";

export type PermissionScope = "guild" | "channels" | "roles" | "messages" | "members" | "events" | "invites";

const ADMIN_EVENT_TYPES = new Map<string, PermissionScope>([
    ["GUILD_UPDATE", "guild"],
    ["CATEGORY_UPSERT", "channels"],
    ["CATEGORY_DELETE", "channels"],
    ["CHANNEL_UPSERT", "channels"],
    ["CHANNEL_DELETE", "channels"],
    ["EVENT_UPSERT", "events"],
    ["EVENT_DELETE", "events"],
    ["TEMPLATE_UPSERT", "guild"],
    ["TEMPLATE_DELETE", "guild"],
    ["ROLE_UPSERT", "roles"],
    ["ROLE_DELETE", "roles"],
    ["INVITE_CREATE", "invites"],
    ["INVITE_REVOKE", "invites"],
    ["BAN_ADD", "members"],
    ["BAN_REMOVE", "members"],
    ["MEMBER_KICK", "members"]
]);

const CHANNEL_PARTICIPATION_EVENT_TYPES = new Set([
    "REACTION_ADD",
    "REACTION_REMOVE",
    "DM_OPEN",
    "CALL_EVENT",
    "THREAD_UPSERT",
    "THREAD_DELETE"
]);

function assertCanParticipateInGuild(state: GuildState, author: string) {
    if (state.bans.has(author)) {
        throw new Error(`User ${author} is banned`);
    }
    if ((state.access === "private" || state.policies.posting === "members") && !state.members.has(author)) {
        throw new Error(`User ${author} is not allowed to publish without guild membership.`);
    }
}

function assertIsMember(state: GuildState, author: string) {
    if (state.bans.has(author)) {
        throw new Error(`User ${author} is banned`);
    }
    if (!state.members.has(author)) {
        throw new Error(`User ${author} is not a member.`);
    }
}

function assertChannelExists(state: GuildState, channelId: string) {
    if (!state.channels.has(channelId)) {
        throw new Error(`Channel ${channelId} does not exist`);
    }
}

function normalizePermission(value: string) {
    return value.replace(/[\s_-]+/g, "").toLowerCase();
}

function permissionSetForMember(state: GuildState, author: string) {
    const permissions = new Set<string>();
    const member = state.members.get(author);
    if (!member) {
        return permissions;
    }

    for (const roleId of member.roles) {
        permissions.add(normalizePermission(roleId));
        const role = state.roles.get(roleId);
        for (const permission of role?.permissions ?? []) {
            permissions.add(normalizePermission(permission));
        }
    }

    return permissions;
}

function hasAnyPermission(permissions: Set<string>, names: string[]) {
    return names.some((name) => permissions.has(normalizePermission(name)));
}

export function canModerateScope(state: GuildState, author: string, scope: PermissionScope) {
    if (state.ownerId === author) {
        return true;
    }

    const permissions = permissionSetForMember(state, author);
    if (hasAnyPermission(permissions, ["owner", "admin", "administrator", "manage_guild", "manage_server"])) {
        return true;
    }

    switch (scope) {
        case "channels":
            return hasAnyPermission(permissions, ["manage_channels", "manageChannels"]);
        case "roles":
            return hasAnyPermission(permissions, ["manage_roles", "manageRoles"]);
        case "messages":
            return hasAnyPermission(permissions, ["manage_messages", "manageMessages"]);
        case "members":
            return hasAnyPermission(permissions, ["moderate_members", "moderateMembers", "kick_members", "ban_members"]);
        case "events":
            return hasAnyPermission(permissions, ["manage_events", "manageEvents"]);
        case "invites":
            return hasAnyPermission(permissions, ["create_instant_invite", "createInvites", "manage_invites", "manageInvites"]);
        case "guild":
            return false;
    }
}

function assertCanModerateScope(state: GuildState, author: string, type: string, scope: PermissionScope) {
    if (!canModerateScope(state, author, scope)) {
        throw new Error(`User ${author} does not have permission for ${type}`);
    }
}

export function validateEvent(state: GuildState, event: GuildEvent) {
    const { body, author } = event;
    const bodyRecord = body as any;

    switch (bodyRecord.type) {
        case "GUILD_CREATE":
            throw new Error("GUILD_CREATE can only appear at seq 0");
        case "CHANNEL_CREATE": {
            assertCanModerateScope(state, author, bodyRecord.type, "channels");
            const channelBody = body as ChannelCreate;
            if (!channelBody.channelId?.trim()) {
                throw new Error("CHANNEL_CREATE requires a channelId");
            }
            if (state.channels.has(channelBody.channelId)) {
                throw new Error(`Channel ${channelBody.channelId} already exists`);
            }
            break;
        }
        case "ROLE_ASSIGN":
        case "ROLE_REVOKE":
        case "BAN_USER":
        case "UNBAN_USER":
        case "EPHEMERAL_POLICY_UPDATE":
            assertCanModerateScope(state, author, bodyRecord.type, bodyRecord.type === "EPHEMERAL_POLICY_UPDATE" ? "channels" : "members");
            break;
        case "REACTION_ADD":
        case "REACTION_REMOVE": {
            assertChannelExists(state, bodyRecord.channelId);
            assertCanParticipateInGuild(state, author);
            if (typeof bodyRecord.reaction !== "string" || !bodyRecord.reaction.trim()) {
                throw new Error(`${bodyRecord.type} requires a reaction`);
            }
            const message = state.messages.get(bodyRecord.messageId);
            if (!message || message.deleted) {
                throw new Error(`Message ${bodyRecord.messageId} does not exist`);
            }
            if (message.channelId !== bodyRecord.channelId) {
                throw new Error(`Message ${bodyRecord.messageId} does not belong to channel ${bodyRecord.channelId}`);
            }
            if (
                bodyRecord.type === "REACTION_REMOVE" &&
                typeof bodyRecord.userId === "string" &&
                bodyRecord.userId !== author &&
                !canModerateScope(state, author, "messages")
            ) {
                throw new Error(`User ${author} cannot remove another user's reaction`);
            }
            break;
        }
        case "MESSAGE":
            const msgBody = body as Message;
            assertChannelExists(state, msgBody.channelId);
            assertCanParticipateInGuild(state, author);
            if (state.messages.has(msgBody.messageId || event.id)) {
                throw new Error(`Message ${msgBody.messageId || event.id} already exists`);
            }
            break;
        case "EDIT_MESSAGE": {
            const editBody = body as EditMessage;
            assertChannelExists(state, editBody.channelId);
            assertCanParticipateInGuild(state, author);
            const message = state.messages.get(editBody.messageId);
            if (!message || message.deleted) {
                throw new Error(`Message ${editBody.messageId} does not exist`);
            }
            if (message.channelId !== editBody.channelId) {
                throw new Error(`Message ${editBody.messageId} does not belong to channel ${editBody.channelId}`);
            }
            if (message.authorId !== author) {
                throw new Error(`User ${author} cannot edit message ${editBody.messageId}`);
            }
            break;
        }
        case "DELETE_MESSAGE": {
            const deleteBody = body as DeleteMessage;
            assertChannelExists(state, deleteBody.channelId);
            assertCanParticipateInGuild(state, author);
            const message = state.messages.get(deleteBody.messageId);
            if (!message || message.deleted) {
                throw new Error(`Message ${deleteBody.messageId} does not exist`);
            }
            if (message.channelId !== deleteBody.channelId) {
                throw new Error(`Message ${deleteBody.messageId} does not belong to channel ${deleteBody.channelId}`);
            }
            if (message.authorId !== author && !canModerateScope(state, author, "messages")) {
                throw new Error(`User ${author} cannot delete message ${deleteBody.messageId}`);
            }
            break;
        }
        case "APP_OBJECT_UPSERT":
        case "APP_OBJECT_DELETE": {
            const appBody = body as AppObjectUpsert | AppObjectDelete;
            assertCanParticipateInGuild(state, author);
            if (!appBody.namespace.trim() || !appBody.objectType.trim() || !appBody.objectId.trim()) {
                throw new Error(`${body.type} requires namespace, objectType, and objectId`);
            }
            const channelId = appBody.channelId || appBody.target?.channelId;
            if (channelId) {
                assertChannelExists(state, channelId);
            }
            const targetMessageId = appBody.target?.messageId;
            if (targetMessageId) {
                const message = state.messages.get(targetMessageId);
                if (!message || message.deleted) {
                    throw new Error(`Target message ${targetMessageId} does not exist`);
                }
                if (channelId && message.channelId !== channelId) {
                    throw new Error(`Target message ${targetMessageId} does not belong to channel ${channelId}`);
                }
            }
            break;
        }
        case "MEMBER_UPDATE":
            if (bodyRecord.userId && bodyRecord.userId !== author) {
                assertCanModerateScope(state, author, bodyRecord.type, "members");
                break;
            }
            assertIsMember(state, author);
            break;
        default: {
            const adminScope = ADMIN_EVENT_TYPES.get(bodyRecord.type);
            if (adminScope) {
                assertCanModerateScope(state, author, bodyRecord.type, adminScope);
                break;
            }

            if (CHANNEL_PARTICIPATION_EVENT_TYPES.has(bodyRecord.type)) {
                if (typeof bodyRecord.channelId === "string" && bodyRecord.channelId) {
                    assertChannelExists(state, bodyRecord.channelId);
                }
                assertCanParticipateInGuild(state, author);
            }
            break;
        }
    }
}
