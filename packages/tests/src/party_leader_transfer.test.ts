import { describe, expect, it } from "vitest";
import {
	applyEvent,
	computeEventId,
	createInitialState,
	deserializeState,
	generatePrivateKey,
	getPublicKey,
	hashObject,
	sign,
	serializeState,
	validateEvent,
	verify,
	type GuildEvent,
} from "@cgp/core";

function keyPair() {
	const privateKey = generatePrivateKey();
	return { privateKey, publicKey: getPublicKey(privateKey) };
}

async function signedEvent(
	privateKey: Uint8Array,
	author: string,
	seq: number,
	prevHash: string | null,
	createdAt: number,
	body: unknown,
): Promise<GuildEvent> {
	const signature = await sign(privateKey, hashObject({ body, author, createdAt }));
	const event = {
		id: "",
		seq,
		prevHash,
		createdAt,
		author,
		body,
		signature,
	} as GuildEvent;
	event.id = computeEventId(event);
	return event;
}

function transferBody(guildId: string, leaderId: string): GuildEvent["body"] {
	return { type: "PARTY_LEADER_TRANSFER", guildId, leaderId } as GuildEvent["body"];
}

function snapshotBody(guildId: string, ownerId: string): GuildEvent["body"] {
	return { type: "PARTY_INVITE_SNAPSHOT", guildId, ownerId } as GuildEvent["body"];
}

async function partyGenesis(owner: ReturnType<typeof keyPair>, guildId = `party:${"a".repeat(64)}`) {
	const genesis = await signedEvent(owner.privateKey, owner.publicKey, 0, null, 1_800_000_000_000, {
		type: "GUILD_CREATE",
		guildId,
		name: "Party metadata",
		access: "public",
		external: { kind: "hollow-party-metadata" },
	});
	return { guildId, genesis, state: createInitialState(genesis) };
}

describe("party metadata leadership transfer", () => {
	it("transfers owner role immutably and lets the successor publish invite snapshots", async () => {
		const owner = keyPair();
		const successor = keyPair();
		const { guildId, genesis, state: initial } = await partyGenesis(owner);
		const join = await signedEvent(successor.privateKey, successor.publicKey, 1, genesis.id, 1_800_000_000_001, {
			type: "MEMBER_JOIN",
			guildId,
		} as GuildEvent["body"]);
		validateEvent(initial, join);
		const memberState = applyEvent(initial, join);
		const transfer = await signedEvent(owner.privateKey, owner.publicKey, 2, join.id, 1_800_000_000_002, transferBody(guildId, successor.publicKey));

		expect(verify(owner.publicKey, hashObject({ body: transfer.body, author: transfer.author, createdAt: transfer.createdAt }), transfer.signature)).toBe(true);
		validateEvent(memberState, transfer);
		const next = applyEvent(memberState, transfer);

		expect(next.ownerId).toBe(successor.publicKey);
		expect(next.members.get(owner.publicKey)?.roles.has("owner")).toBe(false);
		expect(next.members.get(successor.publicKey)?.roles.has("owner")).toBe(true);
		expect(memberState.ownerId).toBe(owner.publicKey);
		expect(memberState.members.get(owner.publicKey)?.roles.has("owner")).toBe(true);
		expect(memberState.members).not.toBe(next.members);

		const successorSnapshot = await signedEvent(successor.privateKey, successor.publicKey, 3, transfer.id, 1_800_000_000_003, snapshotBody(guildId, successor.publicKey));
		expect(() => validateEvent(next, successorSnapshot)).not.toThrow();
		const formerOwnerSnapshot = await signedEvent(owner.privateKey, owner.publicKey, 3, transfer.id, 1_800_000_000_003, snapshotBody(guildId, owner.publicKey));
		expect(() => validateEvent(next, formerOwnerSnapshot)).toThrow(/guild owner/i);
	});

	it("rejects attacker, self, and malformed transfers", async () => {
		const owner = keyPair();
		const successor = keyPair();
		const attacker = keyPair();
		const { guildId, state } = await partyGenesis(owner);
		const attackerTransfer = await signedEvent(attacker.privateKey, attacker.publicKey, 1, state.headHash, 1_800_000_000_001, transferBody(guildId, successor.publicKey));
		const selfTransfer = await signedEvent(owner.privateKey, owner.publicKey, 1, state.headHash, 1_800_000_000_002, transferBody(guildId, owner.publicKey));
		const malformedTransfer = await signedEvent(owner.privateKey, owner.publicKey, 1, state.headHash, 1_800_000_000_003, transferBody(guildId, "not-a-public-key"));

		expect(() => validateEvent(state, attackerTransfer)).toThrow(/party metadata owner/i);
		expect(() => validateEvent(state, selfTransfer)).toThrow(/different account public key/i);
		expect(() => validateEvent(state, malformedTransfer)).toThrow(/different account public key/i);
		expect(() => applyEvent(state, selfTransfer)).toThrow(/invalid party leadership transfer/i);
		expect(state.ownerId).toBe(owner.publicKey);
	});

	it("rejects leadership transfer on non-party guild metadata", async () => {
		const owner = keyPair();
		const successor = keyPair();
		const ordinaryGuild = `b${"b".repeat(63)}`;
		const { state } = await partyGenesis(owner, ordinaryGuild);
		const transfer = await signedEvent(owner.privateKey, owner.publicKey, 1, state.headHash, 1_800_000_000_001, transferBody(ordinaryGuild, successor.publicKey));

		expect(() => validateEvent(state, transfer)).toThrow(/party metadata owner/i);
		expect(() => applyEvent(state, transfer)).toThrow(/invalid party leadership transfer/i);
	});

	it("rejects a banned successor", async () => {
		const owner = keyPair();
		const successor = keyPair();
		const { guildId, genesis, state: initial } = await partyGenesis(owner);
		const ban = await signedEvent(owner.privateKey, owner.publicKey, 1, genesis.id, 1_800_000_000_001, {
			type: "BAN_USER",
			guildId,
			userId: successor.publicKey,
		} as GuildEvent["body"]);
		const bannedState = applyEvent(initial, ban);
		const transfer = await signedEvent(owner.privateKey, owner.publicKey, 2, ban.id, 1_800_000_000_002, transferBody(guildId, successor.publicKey));

		expect(bannedState.bans.has(successor.publicKey)).toBe(true);
		expect(() => validateEvent(bannedState, transfer)).toThrow(/banned/i);
	});

	it("supports a transfer chain and restores successor ownership from serialized state", async () => {
		const owner = keyPair();
		const successor = keyPair();
		const nextLeader = keyPair();
		const { guildId, genesis, state: initial } = await partyGenesis(owner);
		const joinSuccessor = await signedEvent(successor.privateKey, successor.publicKey, 1, genesis.id, 1_800_000_000_001, {
			type: "MEMBER_JOIN",
			guildId,
		} as GuildEvent["body"]);
		const afterJoin = applyEvent(initial, joinSuccessor);
		const firstTransfer = await signedEvent(owner.privateKey, owner.publicKey, 2, joinSuccessor.id, 1_800_000_000_002, transferBody(guildId, successor.publicKey));
		validateEvent(afterJoin, firstTransfer);
		const afterFirstTransfer = applyEvent(afterJoin, firstTransfer);
		const joinNextLeader = await signedEvent(nextLeader.privateKey, nextLeader.publicKey, 3, firstTransfer.id, 1_800_000_000_003, {
			type: "MEMBER_JOIN",
			guildId,
		} as GuildEvent["body"]);
		const afterSecondJoin = applyEvent(afterFirstTransfer, joinNextLeader);
		const secondTransfer = await signedEvent(successor.privateKey, successor.publicKey, 4, joinNextLeader.id, 1_800_000_000_004, transferBody(guildId, nextLeader.publicKey));
		validateEvent(afterSecondJoin, secondTransfer);
		const afterSecondTransfer = applyEvent(afterSecondJoin, secondTransfer);
		const restored = deserializeState(
			serializeState(afterSecondTransfer),
			afterSecondTransfer.headSeq,
			afterSecondTransfer.headHash,
			afterSecondTransfer.createdAt,
		);

		expect(restored.ownerId).toBe(nextLeader.publicKey);
		expect(restored.members.get(owner.publicKey)?.roles.has("owner")).toBe(false);
		expect(restored.members.get(successor.publicKey)?.roles.has("owner")).toBe(false);
		expect(restored.members.get(nextLeader.publicKey)?.roles.has("owner")).toBe(true);
		const snapshot = await signedEvent(nextLeader.privateKey, nextLeader.publicKey, 5, secondTransfer.id, 1_800_000_000_005, snapshotBody(guildId, nextLeader.publicKey));
		expect(() => validateEvent(restored, snapshot)).not.toThrow();
		const staleOwnerSnapshot = await signedEvent(owner.privateKey, owner.publicKey, 5, secondTransfer.id, 1_800_000_000_005, snapshotBody(guildId, owner.publicKey));
		expect(() => validateEvent(restored, staleOwnerSnapshot)).toThrow(/guild owner/i);
	});
});
