import { sha256 } from "@noble/hashes/sha256";
import { hmac } from "@noble/hashes/hmac";
import * as secp from "@noble/secp256k1";
import { ECDH, createPublicKey, createVerify, webcrypto } from "node:crypto";
import stringify from "safe-stable-stringify";
import { HashHex, PublicKeyHex, SignatureHex } from "./types";

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

const secp256k1SpkiPrefix = Buffer.from("3056301006072a8648ce3d020106052b8104000a034200", "hex");
const nativePublicKeyCache = new Map<string, ReturnType<typeof createPublicKey>>();
const nativePublicKeyCacheLimit = 4096;

function compactSignatureToDer(sigHex: string) {
    const bytes = Buffer.from(sigHex, "hex");
    if (bytes.length !== 64) {
        return undefined;
    }

    const derInt = (input: Buffer) => {
        let value = input;
        while (value.length > 1 && value[0] === 0 && (value[1] & 0x80) === 0) {
            value = value.subarray(1);
        }
        if ((value[0] & 0x80) !== 0) {
            value = Buffer.concat([Buffer.from([0]), value]);
        }
        return Buffer.concat([Buffer.from([0x02, value.length]), value]);
    };

    const r = derInt(bytes.subarray(0, 32));
    const s = derInt(bytes.subarray(32, 64));
    return Buffer.concat([Buffer.from([0x30, r.length + s.length]), r, s]);
}

function nativeSecp256k1PublicKey(pubKeyHex: PublicKeyHex) {
    const cached = nativePublicKeyCache.get(pubKeyHex);
    if (cached) {
        return cached;
    }

    const source = Buffer.from(pubKeyHex, "hex");
    const uncompressed = source.length === 65 && source[0] === 4
        ? source
        : ECDH.convertKey(source, "secp256k1", undefined, undefined, "uncompressed");
    const key = createPublicKey({
        key: Buffer.concat([secp256k1SpkiPrefix, Buffer.from(uncompressed)]),
        format: "der",
        type: "spki"
    });

    nativePublicKeyCache.set(pubKeyHex, key);
    if (nativePublicKeyCache.size > nativePublicKeyCacheLimit) {
        const oldestKey = nativePublicKeyCache.keys().next().value;
        if (oldestKey) {
            nativePublicKeyCache.delete(oldestKey);
        }
    }
    return key;
}

export function verifyObject(
    pubKeyHex: PublicKeyHex,
    obj: unknown,
    sigHex: SignatureHex
): boolean {
    const canonical = stringify(obj) ?? "";
    try {
        const derSignature = compactSignatureToDer(sigHex);
        if (derSignature) {
            const verifier = createVerify("sha256");
            verifier.update(canonical, "utf8");
            verifier.end();
            return verifier.verify(nativeSecp256k1PublicKey(pubKeyHex), derSignature);
        }
    } catch {
        // Fall through to the portable noble verifier below.
    }

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
    const key = await webcrypto.subtle.importKey(
        "raw",
        keyBytes.slice(0, 32),
        { name: "AES-GCM" },
        false,
        ["encrypt"]
    );

    const iv = webcrypto.getRandomValues(new Uint8Array(12));
    const encoded = textEncoder.encode(plaintext);

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

    return textDecoder.decode(decrypted);
}

export function generateSymmetricKey(): string {
    const key = webcrypto.getRandomValues(new Uint8Array(32));
    return Buffer.from(key).toString("hex");
}
