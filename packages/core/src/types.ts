export type PublicKeyHex = string;  // lowercase hex, 66 chars including "02"/"03" prefix
export type SignatureHex = string;
export type HashHex = string;       // lowercase hex
export type GuildId = HashHex;      // SHA256 of guild creation event
export type ChannelId = HashHex;    // SHA256 of channel creation event
export type UserId = PublicKeyHex;  // identity == pubkey

export interface GuildEventBodyBase {
    type: string;     // e.g. "GUILD_CREATE", "MESSAGE", ...
    guildId: GuildId; // for all events except GUILD_CREATE itself
    [key: string]: any;
}

export interface GuildEvent {
    id: HashHex;                // SHA256 of canonical encoding of `unsigned`
    seq: number;                // monotonically increasing integer >= 0
    prevHash: HashHex | null;   // null for seq=0, otherwise hash of previous event
    createdAt: number;          // milliseconds since epoch (informational)
    author: UserId;             // public key of signer
    body: EventBody;
    signature: SignatureHex;    // signature over canonical encoding of {seq, prevHash, createdAt, author, body}
}

export interface GuildCreate {
    type: "GUILD_CREATE";
    guildId: GuildId;
    name: string;
    description?: string;
    flags?: {
        allowForksBy?: "any" | "mods" | "owner-only";
    };
}

export interface EphemeralPolicy {
    mode: "infinite" | "rolling-window" | "ttl";
    days?: number;
    seconds?: number;
}

export interface ChannelCreate {
    type: "CHANNEL_CREATE";
    guildId: GuildId;
    channelId: ChannelId;
    name: string;
    kind: "text" | "voice" | "ephemeral-text";
    retention?: EphemeralPolicy;
}

export interface Message {
    type: "MESSAGE";
    guildId: GuildId;
    channelId: ChannelId;
    messageId: HashHex; // SHA256 of (guildId, channelId, seq, author, content)
    content: string;
    replyTo?: HashHex;
}

export interface EditMessage {
    type: "EDIT_MESSAGE";
    guildId: GuildId;
    channelId: ChannelId;
    messageId: HashHex;
    newContent: string;
}

export interface DeleteMessage {
    type: "DELETE_MESSAGE";
    guildId: GuildId;
    channelId: ChannelId;
    messageId: HashHex;
    reason?: string;
}

export interface RoleAssign {
    type: "ROLE_ASSIGN";
    guildId: GuildId;
    userId: UserId;
    roleId: string;
}

export interface RoleRevoke {
    type: "ROLE_REVOKE";
    guildId: GuildId;
    userId: UserId;
    roleId: string;
}

export interface BanUser {
    type: "BAN_USER";
    guildId: GuildId;
    userId: UserId;
    reason?: string;
}

export interface UnbanUser {
    type: "UNBAN_USER";
    guildId: GuildId;
    userId: UserId;
}

export interface Channel {
    id: ChannelId;
    name: string;
    kind: "text" | "voice" | "ephemeral-text";
    retention?: EphemeralPolicy;
}

export interface Role {
    id: string;
    name: string;
    permissions: string[];
}

export interface Member {
    userId: UserId;
    roles: Set<string>;
    nickname?: string;
    joinedAt: number;
}

export interface Ban {
    userId: UserId;
    reason?: string;
    bannedAt: number;
}

export interface SerializableMember {
    userId: UserId;
    roles: string[]; // Serialized as array
    nickname?: string;
    joinedAt: number;
}

export interface SerializableGuildState {
    guildId: GuildId;
    name: string;
    description: string;
    ownerId: UserId;
    channels: Array<[ChannelId, Channel]>; // Map as array of entries
    members: Array<[UserId, SerializableMember]>; // Map as array of entries
    roles: Array<[string, Role]>;           // Map as array of entries
    bans: Array<[UserId, Ban]>;             // Map as array of entries
}

export interface Checkpoint {
    type: "CHECKPOINT";
    guildId: GuildId;
    rootHash: HashHex; // Hash of the serialized state
    seq: number;
    state: SerializableGuildState; // Full state snapshot
}

export interface EphemeralPolicyUpdate {
    type: "EPHEMERAL_POLICY_UPDATE";
    guildId: GuildId;
    channelId: ChannelId;
    retention: EphemeralPolicy;
}

export interface ForkFrom {
    type: "FORK_FROM";
    guildId: GuildId;
    parentGuildId: GuildId;
    parentSeq: number;
    parentRootHash: HashHex;
    note?: string;
}

export type EventBody =
    | GuildCreate
    | ChannelCreate
    | Message
    | EditMessage
    | DeleteMessage
    | ForkFrom
    | RoleAssign
    | RoleRevoke
    | BanUser
    | UnbanUser
    | Checkpoint
    | EphemeralPolicyUpdate;
