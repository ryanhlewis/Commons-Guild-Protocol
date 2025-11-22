import { CgpClient } from "@cgp/client/src/client";
import * as secp from "@noble/secp256k1";

async function main() {
    const privKey = secp.utils.randomPrivateKey();
    const pubKey = Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");

    console.log("Bob starting...");
    const client = new CgpClient({
        relays: ["ws://localhost:7447"],
        keyPair: { pub: pubKey, priv: privKey }
    });

    try {
        await client.connect();
        console.log("Bob connected to relay");

        // Subscribe to a known guild ID (we'll hardcode one for this demo or just listen to everything if we could, but we need a guildId)
        // For the demo, let's have Alice create the guild and print the ID, and Bob needs to know it.
        // OR, we can use a pre-determined guild ID if we can force it.
        // But `createGuild` generates it based on content.

        // Let's make Bob wait for a specific guild ID or just create one himself and listen to it, and Alice joins it.
        // Let's have Bob create the guild "Public Square".

        const guildId = await client.createGuild("Public Square");
        console.log(`Bob created guild: ${guildId}`);

        client.subscribe(guildId);
        console.log("Bob subscribed to guild");

        client.on("event", (ev) => {
            if (ev.body.type === "MESSAGE") {
                console.log(`Bob received message from ${ev.author}: ${ev.body.content}`);
            } else {
                console.log(`Bob received event type: ${ev.body.type}`);
            }
        });

        // Keep alive
        setInterval(() => { }, 1000);

    } catch (e) {
        console.error("Bob error:", e);
    }
}

main();
