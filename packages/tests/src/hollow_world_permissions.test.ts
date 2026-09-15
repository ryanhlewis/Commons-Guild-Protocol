import { expect, it } from 'vitest';
import { applyEvent, createInitialState, computeEventId, validateEvent, hashObject } from '@cgp/core';
import type { GuildEvent } from '@cgp/core';
function event(seq: number, body: any, author = 'owner'): GuildEvent {
	const value = { seq, prevHash: seq ? 'prev' : null, createdAt: 1750000000000 + seq, author, body, signature: 'signature' } as GuildEvent;
	value.id = computeEventId(value); return value;
}
function commit(author = 'owner') {
	const value = { protocol: 'hollow-game-world/1', gameId: 'game', compatibilityId: 'v1', worldId: 'world', branch: 'main', parents: [], createdAt: 1, authorId: author, authorityId: 'peer', treeHash: 'a'.repeat(64), manifest: { cid: 'manifest', sha256: 'a'.repeat(64), bytes: 10, locations: [] } };
	return { ...value, commitId: hashObject(value) };
}
function setup() {
	const guildId = 'review-world';
	const state = createInitialState(event(0, { type: 'GUILD_CREATE', guildId, name: 'Review' }));
	const value = commit();
	const body = { type: 'APP_OBJECT_UPSERT', guildId, namespace: 'app.hollow.game-world', objectType: 'world-commit', objectId: value.commitId, createOnly: true, value };
	return { guildId, state, body };
}
it('enforces immutable world records even when the incoming flag is omitted', () => {
	let { state, body } = setup();
	const first = event(1, body); validateEvent(state, first); state = applyEvent(state, first);
	expect(() => validateEvent(state, event(2, { ...body, createOnly: false }, 'other'))).toThrow(/permission/);
	expect(() => validateEvent(state, event(2, { ...body, createOnly: false }))).toThrow(/immutable/);
});
it('denies unauthorized creation and deletion, and rejects a forged commit hash', () => {
	const { state, body } = setup();
	const value = commit('other');
	expect(() => validateEvent(state, event(1, { ...body, objectId: value.commitId, value }, 'other'))).toThrow(/permission/);
	expect(() => validateEvent(state, event(1, { ...body, value: { ...body.value, treeHash: 'b'.repeat(64) } }))).toThrow(/hash/);
	const next = applyEvent(state, event(1, body));
	const deletion = { type: 'APP_OBJECT_DELETE', guildId: body.guildId, namespace: body.namespace, objectType: body.objectType, objectId: body.objectId };
	expect(() => validateEvent(next, event(2, deletion, 'other'))).toThrow(/permission/);
	expect(() => validateEvent(next, event(2, deletion))).not.toThrow();
});
it('allows a guild-authorized app moderator to create a world commit', () => {
	const { state, body } = setup();
	state.roles.set('world-writers', { id: 'world-writers', name: 'World writers', permissions: ['manage_apps'] } as any);
	state.members.set('writer', { roles: ['world-writers'] } as any);
	const value = commit('writer');
	expect(() => validateEvent(state, event(1, { ...body, objectId: value.commitId, value }, 'writer'))).not.toThrow();
});
