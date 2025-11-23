import { sha256 } from "@noble/hashes/sha256";
import * as secp from "@noble/secp256k1";
import stringify from "safe-stable-stringify";
import { HashHex, PublicKeyHex, SignatureHex } from "./types";

export function hashObject(obj: unknown): HashHex {
    const bytes = new TextEncoder().encode(stringify(obj));
    const h = sha256(bytes);
    return Buffer.from(h).toString("hex");
}

export async function sign(
    privKey: Uint8Array,
    msgHashHex: HashHex
): Promise<SignatureHex> {
    const sig = await secp.signAsync(msgHashHex, privKey);
    return sig.toCompactHex();
}

export function verify(
    pubKeyHex: PublicKeyHex,
    msgHashHex: HashHex,
    sigHex: SignatureHex
): boolean {
    try {
        return secp.verify(sigHex, msgHashHex, pubKeyHex);
    } catch (e) {
        return false;
    }
}

export function getPublicKey(privKey: Uint8Array): PublicKeyHex {
    return Buffer.from(secp.getPublicKey(privKey, true)).toString("hex");
}

export function generatePrivateKey(): Uint8Array {
    return secp.utils.randomPrivateKey();
}

export function getSharedSecret(privKey: Uint8Array, pubKeyHex: PublicKeyHex): Uint8Array {
    return secp.getSharedSecret(privKey, pubKeyHex);
}

import { webcrypto } from "node:crypto";

export async function encrypt(
    keyBytes: Uint8Array,
    plaintext: string
): Promise<{ ciphertext: string; iv: string }> {
    // Use first 32 bytes of shared secret for AES-256
    const key = await webcrypto.subtle.importKey(
        "raw",
        keyBytes.slice(0, 32),
        { name: "AES-GCM" },
        false,
        ["encrypt"]
    );

    const iv = webcrypto.getRandomValues(new Uint8Array(12));
    const encoded = new TextEncoder().encode(plaintext);

    const encrypted = await webcrypto.subtle.encrypt(
        { name: "AES-GCM", iv },
        key,
        encoded
    );

    return {
        ciphertext: Buffer.from(encrypted).toString("base64"),
        iv: Buffer.from(iv).toString("hex")
    };
}

export async function decrypt(
    keyBytes: Uint8Array,
    ciphertextBase64: string,
    ivHex: string
): Promise<string> {
    const key = await webcrypto.subtle.importKey(
        "raw",
        keyBytes.slice(0, 32),
        { name: "AES-GCM" },
        false,
        ["decrypt"]
    );

    const iv = Buffer.from(ivHex, "hex");
    const encrypted = Buffer.from(ciphertextBase64, "base64");

    const decrypted = await webcrypto.subtle.decrypt(
        { name: "AES-GCM", iv },
        key,
        encrypted
    );

    return new TextDecoder().decode(decrypted);
}

export function generateSymmetricKey(): string {
    const key = webcrypto.getRandomValues(new Uint8Array(32));
    return Buffer.from(key).toString("hex");
}
