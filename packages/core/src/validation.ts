import { GuildEvent, Message } from "./types";
import { GuildState } from "./state";

export function validateEvent(state: GuildState, event: GuildEvent) {
    const { body, author } = event;

    const isOwner = state.ownerId === author;
    const member = state.members.get(author);
    const isAdmin = member?.roles.has("admin") || member?.roles.has("owner");
    const hasPermission = isOwner || isAdmin;

    switch (body.type) {
        case "CHANNEL_CREATE":
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
            if (!state.channels.has(msgBody.channelId)) {
                throw new Error(`Channel ${msgBody.channelId} does not exist`);
            }
            if (state.bans.has(author)) {
                throw new Error(`User ${author} is banned`);
            }
            if (state.access === "private" && !state.members.has(author)) {
                throw new Error(`Guild is private. User ${author} is not a member.`);
            }
            break;
    }
}
