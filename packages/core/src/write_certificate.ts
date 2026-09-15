import {
    GuildEvent,
    RelayWriteCertificate,
    RelayWriteProposal,
    RelayWriteQuorumVote,
} from "./types.js";
import { hashObject, verify, verifyObject } from "./crypto.js";
import { computeEventId } from "./log.js";
import { normalizeRelayWriteQuorumPolicy } from "./sfu_authority.js";
import { verifyDeviceAuthorizedObject } from "./device_authority.js";

export function relayWriteProposalId(
    epoch: string,
    proposal: RelayWriteProposal,
) {
    return hashObject({
        protocol: "cgp/write-proposal/1",
        epoch,
        guildId: proposal.guildId,
        headSeq: proposal.headSeq,
        headHash: proposal.headHash,
        body: proposal.body,
        author: proposal.author,
        signature: proposal.signature,
        deviceAuthorization: proposal.deviceAuthorization,
        createdAt: proposal.createdAt,
        clientEventId: proposal.clientEventId,
    });
}

function voteSigningPayload(vote: RelayWriteQuorumVote) {
    return {
        protocol: "cgp/write-vote/1",
        epoch: vote.epoch,
        relayPublicKey: vote.relayPublicKey,
        guildId: vote.guildId,
        headSeq: vote.headSeq,
        headHash: vote.headHash,
        proposalId: vote.proposalId,
        votedAt: vote.votedAt,
    };
}

export function createRelayWriteCertificate(
    policy: {
        epoch: string;
        members: string[];
        requiredVotes: number;
    },
    proposal: RelayWriteProposal,
    votes: RelayWriteQuorumVote[],
): RelayWriteCertificate {
    return {
        protocol: "cgp/write-certificate/1",
        policy: {
            protocol: "cgp/write-quorum/1",
            epoch: policy.epoch,
            members: [...policy.members],
            requiredVotes: policy.requiredVotes,
        },
        proposalId: relayWriteProposalId(policy.epoch, proposal),
        clientEventId: proposal.clientEventId,
        votes: votes.map((vote) => ({ ...vote })),
    };
}

export interface VerifyRelayWriteCertificateOptions {
    expectedGuildId?: string;
    expectedAuthor?: string;
    requireBodyPolicy?: boolean;
}

export function verifyRelayWriteCertificate(
    event: GuildEvent,
    options: VerifyRelayWriteCertificateOptions = {},
) {
    const certificate = event.writeCertificate;
    if (!certificate || certificate.protocol !== "cgp/write-certificate/1") {
        return false;
    }
    let policy;
    try {
        policy = normalizeRelayWriteQuorumPolicy(certificate.policy);
    } catch {
        return false;
    }
    if (
        options.expectedGuildId &&
        event.body.guildId !== options.expectedGuildId
    ) {
        return false;
    }
    if (
        options.expectedAuthor &&
        event.author.toLowerCase() !== options.expectedAuthor.toLowerCase()
    ) {
        return false;
    }
    const bodyPolicy = (event.body as unknown as Record<string, unknown>).certifier;
    if (options.requireBodyPolicy || bodyPolicy !== undefined) {
        try {
            const normalizedBodyPolicy =
                normalizeRelayWriteQuorumPolicy(bodyPolicy);
            if (hashObject(normalizedBodyPolicy) !== hashObject(policy)) {
                return false;
            }
        } catch {
            return false;
        }
    }
    if (
        computeEventId(event) !== event.id ||
        !(event.deviceAuthorization
            ? verifyDeviceAuthorizedObject(
                {
                    body: event.body,
                    author: event.author,
                    createdAt: event.createdAt,
                },
                event.signature,
                event.deviceAuthorization,
                {
                    accountPublicKey: event.author,
                    requiredCapability: "publish",
                },
            ).ok
            : verifyObject(
                event.author,
                {
                    body: event.body,
                    author: event.author,
                    createdAt: event.createdAt,
                },
                event.signature,
            ))
    ) {
        return false;
    }
    const proposal: RelayWriteProposal = {
        guildId: event.body.guildId,
        headSeq: event.seq - 1,
        headHash: event.prevHash,
        body: event.body,
        author: event.author,
        signature: event.signature,
        deviceAuthorization: event.deviceAuthorization,
        createdAt: event.createdAt,
        clientEventId: certificate.clientEventId,
    };
    const proposalId = relayWriteProposalId(policy.epoch, proposal);
    if (certificate.proposalId !== proposalId) {
        return false;
    }
    const memberSet = new Set(policy.members);
    const voters = new Set<string>();
    for (const vote of certificate.votes) {
        const relayPublicKey = vote.relayPublicKey?.toLowerCase();
        if (
            vote.protocol !== "cgp/write-vote/1" ||
            vote.epoch !== policy.epoch ||
            !memberSet.has(relayPublicKey) ||
            voters.has(relayPublicKey) ||
            vote.guildId !== event.body.guildId ||
            vote.headSeq !== event.seq - 1 ||
            vote.headHash !== event.prevHash ||
            vote.proposalId !== proposalId ||
            !Number.isSafeInteger(vote.votedAt) ||
            Math.abs(vote.votedAt - event.createdAt) > 5 * 60 * 1000 ||
            !verify(
                relayPublicKey,
                hashObject(voteSigningPayload(vote)),
                vote.signature,
            )
        ) {
            return false;
        }
        voters.add(relayPublicKey);
    }
    return voters.size >= policy.requiredVotes;
}
