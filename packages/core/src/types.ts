export type PublicKeyHex = string;  // lowercase hex, 66 chars including "02"/"03" prefix
export type SignatureHex = string;
export type HashHex = string;       // lowercase hex
export type GuildId = HashHex;      // stable collision-resistant guild identifier
export type ChannelId = HashHex;    // stable collision-resistant channel identifier
export type UserId = PublicKeyHex;  // identity == pubkey

export type DeviceCapability = "publish" | "read" | "mls" | "device-link";

export interface DeviceAuthorityBinding {
    protocol: "cgp/device-authority/1";
    accountPublicKey: PublicKeyHex;
    authorityPublicKey: PublicKeyHex;
    generation: number;
    activatedAt: number;
    signature: SignatureHex;
}

export interface DeviceCertificate {
    protocol: "cgp/device-certificate/1";
    accountPublicKey: PublicKeyHex;
    authorityPublicKey: PublicKeyHex;
    devicePublicKey: PublicKeyHex;
    serial: string;
    label: string;
    capabilities: DeviceCapability[];
    issuedAt: number;
    expiresAt: number;
    signature: SignatureHex;
}

export interface DeviceRevocationState {
    protocol: "cgp/device-revocation/1";
    accountPublicKey: PublicKeyHex;
    authorityPublicKey: PublicKeyHex;
    generation: number;
    epoch: number;
    updatedAt: number;
    revokedSerials: string[];
    signature: SignatureHex;
}

export interface DeviceAuthorization {
    protocol: "cgp/device-authorization/1";
    binding: DeviceAuthorityBinding;
    certificate: DeviceCertificate;
    revocation: DeviceRevocationState;
}

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
    author: UserId;             // stable account public key
    body: EventBody;
    signature: SignatureHex;    // account signature, or certified device signature when deviceAuthorization is present
    deviceAuthorization?: DeviceAuthorization;
    /**
     * Optional relay quorum proof. It is deliberately excluded from the event
     * id and author signature because it is assembled after relays vote.
     */
    writeCertificate?: RelayWriteCertificate;
}

export interface RelayWriteQuorumPolicy {
    protocol: "cgp/write-quorum/1";
    epoch: string;
    members: PublicKeyHex[];
    requiredVotes: number;
}

export interface RelayWriteQuorumVoteUnsigned {
    protocol: "cgp/write-vote/1";
    epoch: string;
    relayPublicKey: PublicKeyHex;
    guildId: GuildId;
    headSeq: number;
    headHash: HashHex | null;
    proposalId: HashHex;
    votedAt: number;
}

export interface RelayWriteQuorumVote extends RelayWriteQuorumVoteUnsigned {
    signature: SignatureHex;
}

export interface RelayWriteProposal {
    guildId: GuildId;
    headSeq: number;
    headHash: HashHex | null;
    body: unknown;
    author: PublicKeyHex;
    signature: SignatureHex;
    deviceAuthorization?: DeviceAuthorization;
    createdAt: number;
    clientEventId?: string;
    /**
     * In-process relay plugins may request an independently policy-validated
     * write on every witness. This marker is never accepted from client
     * publish frames and is deliberately outside the certificate payload.
     */
    authorizationMode?: "plugin-policy";
}

export interface RelayWriteCertificate {
    protocol: "cgp/write-certificate/1";
    policy: RelayWriteQuorumPolicy;
    proposalId: HashHex;
    clientEventId?: string;
    votes: RelayWriteQuorumVote[];
}

export interface SfuAuthorityMember {
    nodeId: string;
    clusterId: string;
    role: "authority" | "forward-only";
    routeAuthorityPublicKey?: PublicKeyHex;
}

export interface SfuAuthoritySet {
    type: "SFU_AUTHORITY_SET";
    guildId: GuildId;
    epoch: number;
    previousEpoch: number | null;
    notBefore: number;
    overlapUntil: number;
    expiresAt: number;
    certifier: RelayWriteQuorumPolicy;
    authorities: SfuAuthorityMember[];
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
    /**
     * App-object classes that must use a bounded proof-backed exclusive lease.
     * This is useful for scarce one-use resources without making the resource
     * itself part of the CGP core protocol.
     */
    exclusiveAppObjects?: AppObjectLeasePolicy[];
}

export interface AppObjectLeasePolicy {
    namespace: string;
    objectType: string;
    difficultyBits?: number;
    maxLeaseMs?: number;
}

export interface AppObjectLease {
    protocol: "cgp/app-object-lease/1";
    expiresAt: number;
    difficultyBits: number;
    nonce: string;
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
    createOnly?: boolean;
    lease?: AppObjectLease;
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
    sfuAuthoritySets?: SfuAuthoritySet[];
    access: "public" | "private";
    policies?: GuildPolicies;
}

export interface SerializableMessageRef {
    channelId: ChannelId;
    authorId: UserId;
    eventId?: HashHex;
    seq?: number;
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
    createOnly?: boolean;
    lease?: AppObjectLease;
    deviceAuthorization?: DeviceAuthorization;
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

export interface RelayHeadUnsigned {
    protocol: "cgp/0.1";
    relayId: string;
    relayPublicKey: PublicKeyHex;
    guildId: GuildId;
    headSeq: number;
    headHash: HashHex | null;
    prevHash: HashHex | null;
    checkpointSeq?: number | null;
    checkpointHash?: HashHex | null;
    observedAt: number;
}

export interface RelayHead extends RelayHeadUnsigned {
    signature: SignatureHex;
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
    | MemberUpdate
    | SfuAuthoritySet;
