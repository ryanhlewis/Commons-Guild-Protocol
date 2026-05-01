import {
  AppObjectDelete,
  AppObjectUpsert,
  Channel,
  ChannelCreate,
  Checkpoint,
  DeleteMessage,
  EditMessage,
  GuildEvent,
  Message,
} from "./types";
import { checkpointStateRoot, GuildState, serializeState } from "./state";

export type PermissionScope =
  | "guild"
  | "channels"
  | "roles"
  | "messages"
  | "members"
  | "events"
  | "invites"
  | "apps"
  | "webhooks";

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
  ["INVITE_CREATE", "invites"],
  ["INVITE_REVOKE", "invites"],
]);

const CHANNEL_PARTICIPATION_EVENT_TYPES = new Set([
  "REACTION_ADD",
  "REACTION_REMOVE",
  "DM_OPEN",
  "CALL_EVENT",
  "THREAD_UPSERT",
  "THREAD_DELETE",
]);

function assertCanParticipateInGuild(state: GuildState, author: string) {
  if (state.bans.has(author)) {
    throw new Error(`User ${author} is banned`);
  }
  if (
    (state.access === "private" || state.policies.posting === "members") &&
    !state.members.has(author)
  ) {
    throw new Error(
      `User ${author} is not allowed to publish without guild membership.`,
    );
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
  const channel = state.channels.get(channelId);
  if (!channel) {
    throw new Error(`Channel ${channelId} does not exist`);
  }
  return channel;
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

function channelBasePermissions(state: GuildState, author: string) {
  const permissions = new Set<string>([
    "viewchannels",
    "sendmessages",
    "connect",
    "speak",
  ]);
  const member = state.members.get(author);

  if (state.bans.has(author)) {
    return new Set<string>();
  }

  if (state.access === "private" && !member) {
    return new Set<string>();
  }

  if (state.ownerId === author) {
    permissions.add("administrator");
    return permissions;
  }

  for (const roleId of member?.roles ?? []) {
    permissions.add(normalizePermission(roleId));
    const role = state.roles.get(roleId);
    for (const permission of role?.permissions ?? []) {
      permissions.add(normalizePermission(permission));
    }
  }

  return permissions;
}

function normalizedOverwritePermissions(value: unknown) {
  if (Array.isArray(value)) {
    return value
      .filter(
        (entry): entry is string =>
          typeof entry === "string" && entry.trim().length > 0,
      )
      .map(normalizePermission);
  }

  if (value instanceof Set) {
    return [...value]
      .filter(
        (entry): entry is string =>
          typeof entry === "string" && entry.trim().length > 0,
      )
      .map(normalizePermission);
  }

  return [];
}

function applyOverwrite(
  permissions: Set<string>,
  deny: unknown,
  allow: unknown,
) {
  for (const permission of normalizedOverwritePermissions(deny)) {
    permissions.delete(permission);
  }

  for (const permission of normalizedOverwritePermissions(allow)) {
    permissions.add(permission);
  }
}

function everyoneOverwriteIds(state: GuildState) {
  const ids = new Set<string>([state.guildId, "@everyone", "everyone"]);
  for (const [roleId, role] of state.roles) {
    if (
      role.name === "@everyone" ||
      roleId === "@everyone" ||
      roleId === "everyone"
    ) {
      ids.add(roleId);
    }
  }
  return ids;
}

function channelPermissionOverwrites(channel: Channel) {
  return Array.isArray(channel.permissionOverwrites)
    ? channel.permissionOverwrites
    : [];
}

function channelPermissionsForMember(
  state: GuildState,
  channel: Channel,
  author: string,
) {
  const permissions = channelBasePermissions(state, author);
  if (permissions.has("administrator")) {
    return permissions;
  }

  const overwrites = channelPermissionOverwrites(channel);
  const everyoneIds = everyoneOverwriteIds(state);
  const everyoneOverwrite = overwrites.find(
    (overwrite) => overwrite.kind === "role" && everyoneIds.has(overwrite.id),
  );
  if (everyoneOverwrite) {
    applyOverwrite(
      permissions,
      everyoneOverwrite.deny,
      everyoneOverwrite.allow,
    );
  }

  const member = state.members.get(author);
  if (member) {
    const roleDeny = new Set<string>();
    const roleAllow = new Set<string>();
    for (const overwrite of overwrites) {
      if (
        overwrite.kind !== "role" ||
        everyoneIds.has(overwrite.id) ||
        !member.roles.has(overwrite.id)
      ) {
        continue;
      }
      for (const permission of normalizedOverwritePermissions(overwrite.deny)) {
        roleDeny.add(permission);
      }
      for (const permission of normalizedOverwritePermissions(
        overwrite.allow,
      )) {
        roleAllow.add(permission);
      }
    }
    applyOverwrite(permissions, roleDeny, roleAllow);

    const memberOverwrite = overwrites.find(
      (overwrite) => overwrite.kind === "member" && overwrite.id === author,
    );
    if (memberOverwrite) {
      applyOverwrite(permissions, memberOverwrite.deny, memberOverwrite.allow);
    }

    const timedOutUntil = (member as any).timedOutUntil;
    if (
      typeof timedOutUntil === "string" &&
      Date.parse(timedOutUntil) > Date.now()
    ) {
      permissions.delete("sendmessages");
      permissions.delete("speak");
    }
  }

  return permissions;
}

function hasAnyPermission(permissions: Set<string>, names: string[]) {
  return names.some((name) => permissions.has(normalizePermission(name)));
}

export function canReadGuild(state: GuildState, author?: string) {
  if (author && state.bans.has(author)) {
    return false;
  }

  if (state.access === "private") {
    return Boolean(author && state.members.has(author));
  }

  return true;
}

export function canUseChannelPermission(
  state: GuildState,
  author: string | undefined,
  channelId: string,
  permission: string,
) {
  const channel = state.channels.get(channelId);
  if (!channel || !canReadGuild(state, author)) {
    return false;
  }

  const permissions = channelPermissionsForMember(state, channel, author ?? "");
  return hasAnyPermission(permissions, [permission]);
}

export function canViewChannel(
  state: GuildState,
  author: string | undefined,
  channelId: string,
) {
  return canUseChannelPermission(state, author, channelId, "viewChannels");
}

function assertChannelPermission(
  state: GuildState,
  author: string,
  channelId: string,
  permission: string,
  action: string,
) {
  const channel = assertChannelExists(state, channelId);
  const permissions = channelPermissionsForMember(state, channel, author);
  if (!hasAnyPermission(permissions, [permission])) {
    throw new Error(
      `User ${author} does not have ${permission} permission to ${action}`,
    );
  }
}

function canManageMessagesInChannel(
  state: GuildState,
  author: string,
  channelId: string,
) {
  const channel = state.channels.get(channelId);
  return Boolean(
    channel &&
    hasAnyPermission(channelPermissionsForMember(state, channel, author), [
      "manage_messages",
      "manageMessages",
    ]),
  );
}

export function canModerateScope(
  state: GuildState,
  author: string,
  scope: PermissionScope,
) {
  if (state.ownerId === author) {
    return true;
  }

  const permissions = permissionSetForMember(state, author);
  if (
    hasAnyPermission(permissions, [
      "owner",
      "admin",
      "administrator",
      "manage_guild",
      "manage_server",
    ])
  ) {
    return true;
  }

  switch (scope) {
    case "channels":
      return hasAnyPermission(permissions, [
        "manage_channels",
        "manageChannels",
      ]);
    case "roles":
      return hasAnyPermission(permissions, ["manage_roles", "manageRoles"]);
    case "messages":
      return hasAnyPermission(permissions, [
        "manage_messages",
        "manageMessages",
      ]);
    case "members":
      return hasAnyPermission(permissions, [
        "moderate_members",
        "moderateMembers",
        "kick_members",
        "ban_members",
      ]);
    case "events":
      return hasAnyPermission(permissions, ["manage_events", "manageEvents"]);
    case "invites":
      return hasAnyPermission(permissions, [
        "create_instant_invite",
        "createInvites",
        "manage_invites",
        "manageInvites",
      ]);
    case "apps":
      return hasAnyPermission(permissions, [
        "manage_apps",
        "manageApps",
        "manage_integrations",
        "manageIntegrations",
      ]);
    case "webhooks":
      return hasAnyPermission(permissions, [
        "manage_webhooks",
        "manageWebhooks",
      ]);
    case "guild":
      return false;
  }
}

function assertCanModerateScope(
  state: GuildState,
  author: string,
  type: string,
  scope: PermissionScope,
) {
  if (!canModerateScope(state, author, scope)) {
    throw new Error(`User ${author} does not have permission for ${type}`);
  }
}

type MemberModerationPermission =
  | "kickMembers"
  | "banMembers"
  | "moderateMembers";

function hasServerPermission(
  state: GuildState,
  author: string,
  names: string[],
) {
  if (state.ownerId === author) {
    return true;
  }

  const permissions = permissionSetForMember(state, author);
  return hasAnyPermission(permissions, [
    "admin",
    "administrator",
    "manage_guild",
    "manage_server",
    ...names,
  ]);
}

function rolePosition(role: unknown) {
  const position = (role as any)?.position;
  return typeof position === "number" && Number.isFinite(position)
    ? position
    : 0;
}

function highestRolePositionForMember(state: GuildState, userId: string) {
  if (state.ownerId === userId) {
    return Number.POSITIVE_INFINITY;
  }

  const member = state.members.get(userId);
  if (!member) {
    return Number.NEGATIVE_INFINITY;
  }

  let highest = Number.NEGATIVE_INFINITY;
  for (const roleId of member.roles) {
    const role = state.roles.get(roleId);
    if (role) {
      highest = Math.max(highest, rolePosition(role));
    }
  }
  return highest;
}

function canManageRoleTarget(
  state: GuildState,
  author: string,
  roleOrPosition: unknown,
) {
  if (!hasServerPermission(state, author, ["manage_roles", "manageRoles"])) {
    return false;
  }

  if (!state.members.has(author)) {
    return false;
  }

  if (state.ownerId === author) {
    return true;
  }

  if ((roleOrPosition as any)?.managed === true) {
    return false;
  }

  const targetPosition =
    typeof roleOrPosition === "number"
      ? roleOrPosition
      : roleOrPosition
        ? rolePosition(roleOrPosition)
        : Number.NEGATIVE_INFINITY;
  return highestRolePositionForMember(state, author) > targetPosition;
}

function assertCanManageRoleTarget(
  state: GuildState,
  author: string,
  type: string,
  roleOrPosition: unknown,
) {
  if (!canManageRoleTarget(state, author, roleOrPosition)) {
    throw new Error(`User ${author} does not have permission for ${type}`);
  }
}

function normalizedRoleIds(value: unknown) {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter(
    (roleId): roleId is string =>
      typeof roleId === "string" && roleId.trim().length > 0,
  );
}

function canAssignMemberRolesTarget(
  state: GuildState,
  author: string,
  targetId: string,
  roleIds: string[],
) {
  if (
    !targetId ||
    !hasServerPermission(state, author, ["manage_roles", "manageRoles"])
  ) {
    return false;
  }

  const actor = state.members.get(author);
  if (!actor) {
    return false;
  }

  const target = state.members.get(targetId);
  if (!target && state.ownerId !== author) {
    return false;
  }

  if (state.ownerId === targetId) {
    return false;
  }

  if (
    state.ownerId !== author &&
    target &&
    highestRolePositionForMember(state, author) <=
      highestRolePositionForMember(state, targetId)
  ) {
    return false;
  }

  return roleIds.every((roleId) => {
    const role = state.roles.get(roleId);
    if (!role) {
      return state.ownerId === author;
    }
    return canManageRoleTarget(state, author, role);
  });
}

function assertCanAssignMemberRolesTarget(
  state: GuildState,
  author: string,
  targetId: string,
  roleIds: string[],
) {
  if (!canAssignMemberRolesTarget(state, author, targetId, roleIds)) {
    throw new Error(
      `User ${author} does not have permission to update roles for ${targetId}`,
    );
  }
}

function memberModerationPermissionNames(required: MemberModerationPermission) {
  switch (required) {
    case "kickMembers":
      return ["kick_members", "kickMembers"];
    case "banMembers":
      return ["ban_members", "banMembers"];
    case "moderateMembers":
      return ["moderate_members", "moderateMembers"];
  }
}

function canModerateMemberTarget(
  state: GuildState,
  author: string,
  targetId: string,
  required: MemberModerationPermission,
) {
  if (!targetId || author === targetId) {
    return false;
  }

  const actor = state.members.get(author);
  if (!actor) {
    return false;
  }

  if (
    !hasServerPermission(
      state,
      author,
      memberModerationPermissionNames(required),
    )
  ) {
    return false;
  }

  if (state.ownerId === targetId) {
    return false;
  }

  const target = state.members.get(targetId);
  if (!target) {
    return true;
  }

  if (state.ownerId === author) {
    return true;
  }

  return (
    highestRolePositionForMember(state, author) >
    highestRolePositionForMember(state, targetId)
  );
}

function assertCanModerateMemberTarget(
  state: GuildState,
  author: string,
  targetId: string,
  required: MemberModerationPermission,
  action: string,
) {
  if (!canModerateMemberTarget(state, author, targetId, required)) {
    throw new Error(
      `User ${author} does not have permission to ${action} ${targetId}`,
    );
  }
}

function assertHasMemberModerationPermission(
  state: GuildState,
  author: string,
  required: MemberModerationPermission,
  action: string,
) {
  if (
    !state.members.has(author) ||
    !hasServerPermission(
      state,
      author,
      memberModerationPermissionNames(required),
    )
  ) {
    throw new Error(`User ${author} does not have permission to ${action}`);
  }
}

const MEMBER_UPDATE_METADATA_KEYS = new Set([
  "type",
  "guildId",
  "userId",
  "reason",
  "encryptedGroupKey",
  "eventId",
  "clientEventId",
]);
const SELF_MEMBER_UPDATE_KEYS = new Set([
  "nickname",
  "avatar",
  "banner",
  "bio",
  "external",
]);
const MODERATION_MEMBER_UPDATE_KEYS = new Set([
  "timedOutUntil",
  "timeoutUntil",
  "isMuted",
  "isDeafened",
  "isPending",
]);

function memberUpdateKeys(body: Record<string, unknown>) {
  return Object.keys(body).filter(
    (key) => !MEMBER_UPDATE_METADATA_KEYS.has(key) && body[key] !== undefined,
  );
}

function validateMemberUpdate(
  state: GuildState,
  author: string,
  body: Record<string, unknown>,
) {
  const targetId =
    typeof body.userId === "string" && body.userId.trim()
      ? body.userId
      : author;
  const keys = memberUpdateKeys(body);
  if (keys.length === 0) {
    assertIsMember(state, author);
    return;
  }

  if (keys.includes("roleIds") || keys.includes("roles")) {
    const roleIds = normalizedRoleIds(body.roleIds ?? body.roles);
    assertCanAssignMemberRolesTarget(state, author, targetId, roleIds);
    return;
  }

  if (
    author === targetId &&
    keys.every((key) => SELF_MEMBER_UPDATE_KEYS.has(key))
  ) {
    assertIsMember(state, author);
    return;
  }

  const required: MemberModerationPermission = keys.some((key) =>
    MODERATION_MEMBER_UPDATE_KEYS.has(key),
  )
    ? "moderateMembers"
    : "moderateMembers";
  assertCanModerateMemberTarget(
    state,
    author,
    targetId,
    required,
    "update member",
  );
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
      assertCanAssignMemberRolesTarget(
        state,
        author,
        bodyRecord.userId,
        typeof bodyRecord.roleId === "string" ? [bodyRecord.roleId] : [],
      );
      break;
    case "ROLE_UPSERT": {
      const current =
        typeof bodyRecord.roleId === "string"
          ? state.roles.get(bodyRecord.roleId)
          : undefined;
      const target =
        current ??
        (typeof bodyRecord.position === "number"
          ? bodyRecord.position
          : Number.NEGATIVE_INFINITY);
      assertCanManageRoleTarget(state, author, bodyRecord.type, target);
      break;
    }
    case "ROLE_DELETE":
      assertCanManageRoleTarget(
        state,
        author,
        bodyRecord.type,
        typeof bodyRecord.roleId === "string"
          ? state.roles.get(bodyRecord.roleId)
          : undefined,
      );
      break;
    case "BAN_USER":
    case "BAN_ADD":
      assertCanModerateMemberTarget(
        state,
        author,
        bodyRecord.userId,
        "banMembers",
        "ban",
      );
      break;
    case "UNBAN_USER":
    case "BAN_REMOVE":
      assertHasMemberModerationPermission(
        state,
        author,
        "banMembers",
        "unban members",
      );
      break;
    case "EPHEMERAL_POLICY_UPDATE":
      assertCanModerateScope(
        state,
        author,
        bodyRecord.type,
        bodyRecord.type === "EPHEMERAL_POLICY_UPDATE" ? "channels" : "members",
      );
      break;
    case "MEMBER_KICK":
      assertCanModerateMemberTarget(
        state,
        author,
        bodyRecord.userId,
        "kickMembers",
        "kick",
      );
      break;
    case "REACTION_ADD":
    case "REACTION_REMOVE": {
      assertCanParticipateInGuild(state, author);
      assertChannelPermission(
        state,
        author,
        bodyRecord.channelId,
        "viewChannels",
        "react in channel",
      );
      assertChannelPermission(
        state,
        author,
        bodyRecord.channelId,
        "sendMessages",
        "react in channel",
      );
      if (
        typeof bodyRecord.reaction !== "string" ||
        !bodyRecord.reaction.trim()
      ) {
        throw new Error(`${bodyRecord.type} requires a reaction`);
      }
      const message = state.messages.get(bodyRecord.messageId);
      if (!message || message.deleted) {
        throw new Error(`Message ${bodyRecord.messageId} does not exist`);
      }
      if (message.channelId !== bodyRecord.channelId) {
        throw new Error(
          `Message ${bodyRecord.messageId} does not belong to channel ${bodyRecord.channelId}`,
        );
      }
      if (
        bodyRecord.type === "REACTION_REMOVE" &&
        typeof bodyRecord.userId === "string" &&
        bodyRecord.userId !== author &&
        !canManageMessagesInChannel(state, author, bodyRecord.channelId)
      ) {
        throw new Error(`User ${author} cannot remove another user's reaction`);
      }
      break;
    }
    case "MESSAGE":
      const msgBody = body as Message;
      assertCanParticipateInGuild(state, author);
      assertChannelPermission(
        state,
        author,
        msgBody.channelId,
        "viewChannels",
        "send into channel",
      );
      assertChannelPermission(
        state,
        author,
        msgBody.channelId,
        "sendMessages",
        "send into channel",
      );
      if (state.messages.has(msgBody.messageId || event.id)) {
        throw new Error(
          `Message ${msgBody.messageId || event.id} already exists`,
        );
      }
      break;
    case "EDIT_MESSAGE": {
      const editBody = body as EditMessage;
      assertCanParticipateInGuild(state, author);
      assertChannelPermission(
        state,
        author,
        editBody.channelId,
        "viewChannels",
        "edit in channel",
      );
      const message = state.messages.get(editBody.messageId);
      if (!message || message.deleted) {
        throw new Error(`Message ${editBody.messageId} does not exist`);
      }
      if (message.channelId !== editBody.channelId) {
        throw new Error(
          `Message ${editBody.messageId} does not belong to channel ${editBody.channelId}`,
        );
      }
      if (message.authorId !== author) {
        throw new Error(
          `User ${author} cannot edit message ${editBody.messageId}`,
        );
      }
      break;
    }
    case "DELETE_MESSAGE": {
      const deleteBody = body as DeleteMessage;
      assertCanParticipateInGuild(state, author);
      assertChannelPermission(
        state,
        author,
        deleteBody.channelId,
        "viewChannels",
        "delete in channel",
      );
      const message = state.messages.get(deleteBody.messageId);
      if (!message || message.deleted) {
        throw new Error(`Message ${deleteBody.messageId} does not exist`);
      }
      if (message.channelId !== deleteBody.channelId) {
        throw new Error(
          `Message ${deleteBody.messageId} does not belong to channel ${deleteBody.channelId}`,
        );
      }
      if (
        message.authorId !== author &&
        !canManageMessagesInChannel(state, author, deleteBody.channelId)
      ) {
        throw new Error(
          `User ${author} cannot delete message ${deleteBody.messageId}`,
        );
      }
      break;
    }
    case "APP_OBJECT_UPSERT":
    case "APP_OBJECT_DELETE": {
      const appBody = body as AppObjectUpsert | AppObjectDelete;
      assertCanParticipateInGuild(state, author);
      if (
        !appBody.namespace.trim() ||
        !appBody.objectType.trim() ||
        !appBody.objectId.trim()
      ) {
        throw new Error(
          `${body.type} requires namespace, objectType, and objectId`,
        );
      }
      const channelId = appBody.channelId || appBody.target?.channelId;
      if (channelId) {
        assertChannelPermission(
          state,
          author,
          channelId,
          "viewChannels",
          "publish app object in channel",
        );
      }
      const targetMessageId = appBody.target?.messageId;
      if (targetMessageId) {
        const message = state.messages.get(targetMessageId);
        if (!message || message.deleted) {
          throw new Error(`Target message ${targetMessageId} does not exist`);
        }
        if (channelId && message.channelId !== channelId) {
          throw new Error(
            `Target message ${targetMessageId} does not belong to channel ${channelId}`,
          );
        }
      }
      break;
    }
    case "MEMBER_UPDATE":
      validateMemberUpdate(state, author, bodyRecord);
      break;
    case "CHECKPOINT": {
      const checkpoint = body as Checkpoint;
      if (checkpoint.seq !== event.seq) {
        throw new Error("CHECKPOINT seq must match event seq");
      }
      if (checkpoint.guildId !== state.guildId) {
        throw new Error("CHECKPOINT guildId must match current state");
      }
      if (checkpoint.state.guildId !== state.guildId) {
        throw new Error("CHECKPOINT state guildId must match current state");
      }
      const currentRoot = checkpointStateRoot(serializeState(state));
      if (checkpoint.rootHash !== currentRoot) {
        throw new Error("CHECKPOINT rootHash does not match current state");
      }
      if (checkpointStateRoot(checkpoint.state) !== checkpoint.rootHash) {
        throw new Error("CHECKPOINT state does not match rootHash");
      }
      break;
    }
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
