import { describe, it, expect } from "vitest";
import { CgpClient } from "@cgp/client";
import { generatePrivateKey, getPublicKey } from "@cgp/core";

describe("End-to-End Encryption", () => {
    const log = (msg: string) => process.stdout.write(`[E2EE] ${msg}\n`);

    it("encrypts and decrypts messages in private DMs", async () => {
        const P1_PORT = 9411;
        const P2_PORT = 9412;

        // Peer 1 (Alice)
        const priv1 = generatePrivateKey();
        const pub1 = getPublicKey(priv1);
        const peer1 = new CgpClient({ relays: [], keyPair: { pub: pub1, priv: priv1 } });

        // Peer 2 (Bob)
        const priv2 = generatePrivateKey();
        const pub2 = getPublicKey(priv2);
        const peer2 = new CgpClient({ relays: [], keyPair: { pub: pub2, priv: priv2 } });

        await peer1.listen(P1_PORT);
        await peer2.listen(P2_PORT);
        await peer1.connectToPeer(`ws://localhost:${P2_PORT}`);

        // Wait for connection
        await new Promise(r => setTimeout(r, 500));

        // 1. Alice creates PRIVATE Guild
        const guildId = await peer1.createGuild("Secret Chat", "Private", "private");
        const channelId = await peer1.createChannel(guildId, "general", "text");
        log(`Private Guild Created: ${guildId}`);

        // 2. Alice invites Bob
        await peer1.assignRole(guildId, pub2, "member");

        // Wait for sync
        await new Promise(r => setTimeout(r, 1000));

        // 3. Alice sends a message
        const secretMessage = "The eagle flies at midnight";
        await peer1.sendMessage(guildId, channelId, secretMessage);
        log("Message sent");

        await new Promise(r => setTimeout(r, 500));

        // 4. Verify Bob received it
        // We need to capture the event to check if it's encrypted
        // We can use `peer2.on("event", ...)` but it's async.
        // Or check internal state if we could, but messages aren't in state.
        // We can use a spy or just check if `decryptMessage` works.

        // Let's spy on `peer2` emit? No, let's just use `decryptMessage` on the last event if we can find it.
        // But `CgpClient` doesn't expose event history easily.
        // Wait, `peer2` emits "event".

        let receivedEvent: any;
        peer2.on("event", (e) => {
            if (e.body.type === "MESSAGE") receivedEvent = e;
        });

        // Resend to capture
        await peer1.sendMessage(guildId, channelId, "Second secret");
        await new Promise(r => setTimeout(r, 500));

        expect(receivedEvent).toBeDefined();
        expect(receivedEvent.body.content).not.toBe("Second secret"); // Should be encrypted
        expect(receivedEvent.body.encrypted).toBe(true);
        expect(receivedEvent.body.iv).toBeDefined();

        log(`Ciphertext: ${receivedEvent.body.content}`);

        // 5. Verify Bob can decrypt
        const decrypted = await peer2.decryptMessage(receivedEvent);
        log(`Bob Decrypted: ${decrypted}`);
        expect(decrypted).toBe("Second secret");

        // 6. Verify Alice can decrypt her own message
        const decryptedAlice = await peer1.decryptMessage(receivedEvent);
        log(`Alice Decrypted: ${decryptedAlice}`);
        expect(decryptedAlice).toBe("Second secret");

        peer1.close();
        peer2.close();
    }, 30000);
});
