import { describe, it, expect } from "vitest";
import { CgpClient } from "@cgp/client";
import { generatePrivateKey, getPublicKey } from "@cgp/core";

describe("DM Security Vulnerabilities", () => {
    const log = (msg: string) => process.stdout.write(`[DM Security] ${msg}\n`);

    it("prevents Public Write in Private Guilds", async () => {
        const P1_PORT = 9401;
        const P2_PORT = 9402;

        // Peer 1 (Owner)
        const priv1 = generatePrivateKey();
        const pub1 = getPublicKey(priv1);
        const peer1 = new CgpClient({ relays: [], keyPair: { pub: pub1, priv: priv1 } });

        // Peer 2 (Attacker / Non-Member)
        const priv2 = generatePrivateKey();
        const pub2 = getPublicKey(priv2);
        const peer2 = new CgpClient({ relays: [], keyPair: { pub: pub2, priv: priv2 } });

        await peer1.listen(P1_PORT);
        await peer2.listen(P2_PORT);
        await peer1.connectToPeer(`ws://localhost:${P2_PORT}`);

        // Wait for connection
        await new Promise(r => setTimeout(r, 500));

        // 1. Honest Peer creates PRIVATE Guild
        const guildId = await peer1.createGuild("Private DM Guild", "Private", "private");
        const channelId = await peer1.createChannel(guildId, "general", "text");
        log(`Private Guild Created: ${guildId}`);

        // Wait for sync
        await new Promise(r => setTimeout(r, 500));

        // 2. Attacker (Non-Member) tries to send a message
        log("Attacker attempting to post to private guild...");
        let error: any;
        try {
            await peer2.sendMessage(guildId, channelId, "I am spamming your DM!");
        } catch (e) {
            error = e;
            log(`Post failed as expected: ${e}`);
        }

        await new Promise(r => setTimeout(r, 500));

        // Verify Peer 1 REJECTED it
        const state1 = peer1.getGuildState(guildId);

        log(`Peer 1 Head Seq: ${state1?.headSeq}`);
        // Seq 0: GuildCreate
        // Seq 1: ChannelCreate
        // Seq 2: Message (Should be rejected)

        expect(state1?.headSeq).toBe(1); // Should stay at 1

        // 3. Now invite Attacker (Assign Role)
        log("Inviting Attacker (Assigning Role)...");
        await peer1.assignRole(guildId, pub2, "member"); // "member" role doesn't exist but assignment makes them a member

        await new Promise(r => setTimeout(r, 500));

        // 4. Attacker tries again
        log("Attacker attempting to post AFTER invite...");
        await peer2.sendMessage(guildId, channelId, "Thanks for the invite!");

        await new Promise(r => setTimeout(r, 500));
        const state1After = peer1.getGuildState(guildId);
        log(`Peer 1 Head Seq After Invite: ${state1After?.headSeq}`);

        // Seq 2: RoleAssign
        // Seq 3: Message (Should be accepted)
        expect(state1After?.headSeq).toBe(3);

        peer1.close();
        peer2.close();
    }, 30000);
});
