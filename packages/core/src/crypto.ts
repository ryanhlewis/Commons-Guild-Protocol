import { sha256 } from "@noble/hashes/sha256";
import { hmac } from "@noble/hashes/hmac";
import * as secp from "@noble/secp256k1";
import stringify from "safe-stable-stringify";
import { HashHex, PublicKeyHex, SignatureHex } from "./types.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

if (!secp.etc.hmacSha256Sync) {
    secp.etc.hmacSha256Sync = (key, ...msgs) => hmac(sha256, key, secp.etc.concatBytes(...msgs));
}

export function hashObject(obj: unknown): HashHex {
    const bytes = textEncoder.encode(stringify(obj));
    const h = sha256(bytes);
    return Buffer.from(h).toString("hex");
}

export async function sign(
    privKey: Uint8Array,
    msgHashHex: HashHex
): Promise<SignatureHex> {
    if (typeof process !== "undefined" && Boolean(process.versions?.node)) {
        const sig = secp.sign(msgHashHex, privKey);
        return sig.toCompactHex();
    }
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

export function verifyObject(
    pubKeyHex: PublicKeyHex,
    obj: unknown,
    sigHex: SignatureHex
): boolean {
    return verify(pubKeyHex, hashObject(obj), sigHex);
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

export async function encrypt(
    keyBytes: Uint8Array,
    plaintext: string
): Promise<{ ciphertext: string; iv: string }> {
    // Use first 32 bytes of shared secret for AES-256
    const key = await globalThis.crypto.subtle.importKey(
        "raw",
        keyBytes.slice(0, 32),
        { name: "AES-GCM" },
        false,
        ["encrypt"]
    );

    const iv = globalThis.crypto.getRandomValues(new Uint8Array(12));
    const encoded = textEncoder.encode(plaintext);

    const encrypted = await globalThis.crypto.subtle.encrypt(
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
    const key = await globalThis.crypto.subtle.importKey(
        "raw",
        keyBytes.slice(0, 32),
        { name: "AES-GCM" },
        false,
        ["decrypt"]
    );

    const iv = Buffer.from(ivHex, "hex");
    const encrypted = Buffer.from(ciphertextBase64, "base64");

    const decrypted = await globalThis.crypto.subtle.decrypt(
        { name: "AES-GCM", iv },
        key,
        encrypted
    );

    return textDecoder.decode(decrypted);
}

export function generateSymmetricKey(): string {
    const key = globalThis.crypto.getRandomValues(new Uint8Array(32));
    return Buffer.from(key).toString("hex");
}
