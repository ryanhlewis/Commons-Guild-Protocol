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
