export type PublicKeyHex = string;  // lowercase hex, 66 chars including "02"/"03" prefix
export type SignatureHex = string;
export type HashHex = string;       // lowercase hex
export type GuildId = HashHex;      // stable collision-resistant guild identifier
export type ChannelId = HashHex;    // stable collision-resistant channel identifier
export type UserId = PublicKeyHex;  // identity == pubkey

export interface GuildEventBodyBase {
    type: string;     // e.g. "GUILD_CREATE", "MESSAGE", ...
    guildId: GuildId; // target guild log
    [key: string]: any;
}

export interface MemberUpdate {
    type: "MEMBER_UPDATE";
    guildId: GuildId;
    userId?: UserId;
    nickname?: string;
    avatar?: string;
    banner?: string;
    bio?: string;
    external?: any;
}

export interface GuildEvent {
    id: HashHex;                // SHA256 of canonical encoding of `unsigned`
    seq: number;                // monotonically increasing integer >= 0
    prevHash: HashHex | null;   // null for seq=0, otherwise hash of previous event
    createdAt: number;          // milliseconds since epoch (informational)
    author: UserId;             // public key of signer
    body: EventBody;
    signature: SignatureHex;    // signature over canonical encoding of {body, author, createdAt}
}

export interface GuildCreate {
    type: "GUILD_CREATE";
    guildId: GuildId;
    name: string;
    description?: string;
    flags?: {
        allowForksBy?: "any" | "mods" | "owner-only";
    };
    access?: "public" | "private"; // Default public
    policies?: GuildPolicies;
    encryptedGroupKey?: string;
}

export interface GuildPolicies {
    /**
     * Who may publish user-authored channel/application objects into the guild.
     * "public" keeps open community behavior; "members" requires explicit membership
     * even when the guild profile itself is public.
     */
    posting?: "public" | "members";
}

export interface GuildUpdate {
    type: "GUILD_UPDATE";
    guildId: GuildId;
    name?: string;
    description?: string;
    access?: "public" | "private";
    policies?: GuildPolicies;
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
    categoryId?: string;
    description?: string;
    topic?: string;
    position?: number;
    permissionOverwrites?: PermissionOverwrite[];
}

export interface ChannelUpsert {
    type: "CHANNEL_UPSERT";
    guildId: GuildId;
    channelId: ChannelId;
    name?: string;
    kind?: "text" | "voice" | "ephemeral-text" | string;
    retention?: EphemeralPolicy;
    categoryId?: string;
    description?: string;
    topic?: string;
    position?: number;
    permissionOverwrites?: any[];
}

export interface ChannelDelete {
    type: "CHANNEL_DELETE";
    guildId: GuildId;
    channelId: ChannelId;
}

export interface Message {
    type: "MESSAGE";
    guildId: GuildId;
    channelId: ChannelId;
    messageId: HashHex; // stable client-chosen id; SHOULD hash a collision-resistant message preimage
    content: string;
    replyTo?: HashHex;
    attachments?: AttachmentRef[];
    iv?: string;
    encrypted?: boolean;
    external?: any;
}

export interface AttachmentRef {
    id?: string;
    url?: string;
    type?: "image" | "video" | "audio" | "file" | string;
    name?: string;
    mimeType?: string;
    size?: number;
    width?: number;
    height?: number;
    hash?: HashHex;
    encrypted?: boolean;
    scheme?: string;
    iv?: string;
    content?: string;
    external?: any;
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

export interface ReactionAdd {
    type: "REACTION_ADD";
    guildId: GuildId;
    channelId: ChannelId;
    messageId: HashHex;
    reaction: string;
}

export interface ReactionRemove {
    type: "REACTION_REMOVE";
    guildId: GuildId;
    channelId: ChannelId;
    messageId: HashHex;
    reaction: string;
    userId?: UserId;
}

export interface AppObjectTarget {
    channelId?: ChannelId;
    messageId?: HashHex;
    userId?: UserId;
    [key: string]: any;
}

export interface AppObjectUpsert {
    type: "APP_OBJECT_UPSERT";
    guildId: GuildId;
    namespace: string;
    objectType: string;
    objectId: string;
    channelId?: ChannelId;
    target?: AppObjectTarget;
    value?: any;
}

export interface AppObjectDelete {
    type: "APP_OBJECT_DELETE";
    guildId: GuildId;
    namespace: string;
    objectType: string;
    objectId: string;
    channelId?: ChannelId;
    target?: AppObjectTarget;
}

export interface RoleAssign {
    type: "ROLE_ASSIGN";
    guildId: GuildId;
    userId: UserId;
    roleId: string;
    encryptedGroupKey?: string;
}

export interface RoleRevoke {
    type: "ROLE_REVOKE";
    guildId: GuildId;
    userId: UserId;
    roleId: string;
}

export interface RoleUpsert {
    type: "ROLE_UPSERT";
    guildId: GuildId;
    roleId: string;
    name?: string;
    permissions?: string[];
    color?: string;
    icon?: string;
    position?: number;
    mentionable?: boolean;
    hoist?: boolean;
    managed?: boolean;
}

export interface RoleDelete {
    type: "ROLE_DELETE";
    guildId: GuildId;
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

export interface BanAdd {
    type: "BAN_ADD";
    guildId: GuildId;
    userId: UserId;
    reason?: string;
    expiresAt?: string;
}

export interface BanRemove {
    type: "BAN_REMOVE";
    guildId: GuildId;
    userId: UserId;
}

export interface MemberKick {
    type: "MEMBER_KICK";
    guildId: GuildId;
    userId: UserId;
    reason?: string;
}

export interface Channel {
    id: ChannelId;
    name: string;
    kind: "text" | "voice" | "ephemeral-text" | string;
    retention?: EphemeralPolicy;
    categoryId?: string;
    description?: string;
    topic?: string;
    position?: number;
    permissionOverwrites?: PermissionOverwrite[];
}

export interface PermissionOverwrite {
    id: string;
    kind: "role" | "member";
    allow?: string[];
    deny?: string[];
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
    avatar?: string;
    banner?: string;
    bio?: string;
    joinedAt: number;
}

export interface Ban {
    userId: UserId;
    reason?: string;
    expiresAt?: string;
    bannedAt: number;
}

export interface SerializableMember {
    userId: UserId;
    roles: string[]; // Serialized as array
    nickname?: string;
    avatar?: string;
    banner?: string;
    bio?: string;
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
    messages?: Array<[HashHex, SerializableMessageRef]>;
    appObjects?: Array<[string, AppObjectStateRef]>;
    access: "public" | "private";
    policies?: GuildPolicies;
}

export interface SerializableMessageRef {
    channelId: ChannelId;
    authorId: UserId;
    deleted?: boolean;
    reactions?: Record<string, UserId[]>;
}

export interface AppObjectStateRef {
    namespace: string;
    objectType: string;
    objectId: string;
    channelId?: ChannelId;
    target?: AppObjectTarget;
    value?: any;
    authorId: UserId;
    updatedAt: number;
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
    | GuildUpdate
    | ChannelCreate
    | ChannelUpsert
    | ChannelDelete
    | Message
    | EditMessage
    | DeleteMessage
    | ReactionAdd
    | ReactionRemove
    | AppObjectUpsert
    | AppObjectDelete
    | ForkFrom
    | RoleUpsert
    | RoleDelete
    | RoleAssign
    | RoleRevoke
    | BanUser
    | UnbanUser
    | BanAdd
    | BanRemove
    | MemberKick
    | Checkpoint
    | EphemeralPolicyUpdate
    | MemberUpdate;
