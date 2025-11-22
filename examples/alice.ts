import { CgpClient } from "@cgp/client/src/client";
import * as secp from "@noble/secp256k1";

async function main() {
    // Get guild ID from args
    const guildId = process.argv[2];
    if (!guildId) {
        console.error("Please provide guild ID as argument");
        process.exit(1);
    }

    const privKey = secp.utils.randomPrivateKey();
    const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");

    console.log("Alice starting...");
    const client = new CgpClient({
        relays: ["ws://localhost:7447"],
        keyPair: { pub: pubKey, priv: privKey }
    });

    try {
        await client.connect();
        console.log("Alice connected to relay");

        console.log(`Alice sending message to guild ${guildId}...`);
        await client.publish({
            type: "MESSAGE",
            guildId,
            channelId: "general",
            content: "Hello Bob, this is a real message over WebSocket!"
        });
        console.log("Alice sent message");

        // Close after a bit
        setTimeout(() => process.exit(0), 1000);

    } catch (e) {
        console.error("Alice error:", e);
    }
}

main();
