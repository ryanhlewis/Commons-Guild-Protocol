import { AppObjectDelete, AppObjectUpsert, ChannelCreate, DeleteMessage, EditMessage, GuildEvent, Message } from "./types";
import { GuildState } from "./state";

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

export function validateEvent(state: GuildState, event: GuildEvent) {
    const { body, author } = event;

    const isOwner = state.ownerId === author;
    const member = state.members.get(author);
    const isAdmin = member?.roles.has("admin") || member?.roles.has("owner");
    const hasPermission = isOwner || isAdmin;

    switch (body.type) {
        case "GUILD_CREATE":
            throw new Error("GUILD_CREATE can only appear at seq 0");
        case "CHANNEL_CREATE": {
            if (!hasPermission) {
                throw new Error(`User ${author} does not have permission for ${body.type}`);
            }
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
            if (!hasPermission) {
                throw new Error(`User ${author} does not have permission for ${body.type}`);
            }
            break;
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
            if (message.authorId !== author && !hasPermission) {
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
            assertIsMember(state, author);
            break;
    }
}
