import * as secp from "@noble/secp256k1";
import { webcrypto } from "node:crypto";

async function testCrypto() {
    const priv1 = secp.utils.randomPrivateKey();
    const pub1 = secp.getPublicKey(priv1, true);
    const priv2 = secp.utils.randomPrivateKey();
    const pub2 = secp.getPublicKey(priv2, true);

    const shared1 = secp.getSharedSecret(priv1, pub2);
    const shared2 = secp.getSharedSecret(priv2, pub1);

    console.log("Shared secrets match:", Buffer.from(shared1).equals(Buffer.from(shared2)));

    // AES-GCM
    const key = await webcrypto.subtle.importKey(
        "raw",
        shared1.slice(0, 32), // Use first 32 bytes
        { name: "AES-GCM" },
        false,
        ["encrypt", "decrypt"]
    );

    const iv = webcrypto.getRandomValues(new Uint8Array(12));
    const encoded = new TextEncoder().encode("Hello World");

    const encrypted = await webcrypto.subtle.encrypt(
        { name: "AES-GCM", iv },
        key,
        encoded
    );

    const decrypted = await webcrypto.subtle.decrypt(
        { name: "AES-GCM", iv },
        key,
        encrypted
    );

    console.log("Decrypted:", new TextDecoder().decode(decrypted));
}

testCrypto().catch(console.error);
