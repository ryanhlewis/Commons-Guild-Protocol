import { verifyObject } from "./crypto.js";
import type {
    DeviceAuthorization,
    DeviceCapability,
    DeviceCertificate,
    DeviceRevocationState,
    PublicKeyHex,
} from "./types.js";

const PUBLIC_KEY_PATTERN = /^(?:02|03)[a-f0-9]{64}$/;
const SIGNATURE_PATTERN = /^[a-f0-9]{128}$/;
const SERIAL_PATTERN = /^[a-f0-9]{32}$/;
const MAX_CLOCK_SKEW_MS = 5 * 60 * 1000;
const MAX_REVOKED_CERTIFICATES = 4096;
const CAPABILITIES = new Set<DeviceCapability>([
    "publish",
    "read",
    "mls",
    "device-link",
]);

export interface DeviceAuthorizationVerification {
    ok: boolean;
    accountPublicKey?: PublicKeyHex;
    authorityPublicKey?: PublicKeyHex;
    devicePublicKey?: PublicKeyHex;
    revocationEpoch?: number;
    error?: string;
}

export interface DeviceAuthorityPin {
    accountPublicKey: PublicKeyHex;
    authorityPublicKey: PublicKeyHex;
    generation: number;
    activatedAt: number;
    revocationEpoch: number;
    observedAt: number;
}

function normalizedHex(value: unknown, pattern: RegExp, name: string) {
    const normalized =
        typeof value === "string" ? value.trim().toLowerCase() : "";
    if (!pattern.test(normalized)) {
        throw new Error(`${name} is invalid`);
    }
    return normalized;
}

function normalizedInteger(value: unknown, name: string, minimum = 0) {
    const normalized = Number(value);
    if (!Number.isSafeInteger(normalized) || normalized < minimum) {
        throw new Error(`${name} is invalid`);
    }
    return normalized;
}

function normalizeCapabilities(value: unknown) {
    if (!Array.isArray(value) || value.length < 1 || value.length > CAPABILITIES.size) {
        throw new Error("Device capabilities are invalid");
    }
    const normalized = Array.from(
        new Set(value.map((entry) => String(entry))),
    ).sort() as DeviceCapability[];
    if (normalized.some((entry) => !CAPABILITIES.has(entry))) {
        throw new Error("Device capabilities are invalid");
    }
    return normalized;
}

function normalizeAuthorization(value: DeviceAuthorization): DeviceAuthorization {
    if (!value || value.protocol !== "cgp/device-authorization/1") {
        throw new Error("Device authorization protocol is unsupported");
    }
    const binding = {
        protocol: value.binding?.protocol,
        accountPublicKey: normalizedHex(
            value.binding?.accountPublicKey,
            PUBLIC_KEY_PATTERN,
            "Authority account public key",
        ),
        authorityPublicKey: normalizedHex(
            value.binding?.authorityPublicKey,
            PUBLIC_KEY_PATTERN,
            "Authority public key",
        ),
        generation: normalizedInteger(
            value.binding?.generation,
            "Authority generation",
            1,
        ),
        activatedAt: normalizedInteger(
            value.binding?.activatedAt,
            "Authority activation time",
            1,
        ),
        signature: normalizedHex(
            value.binding?.signature,
            SIGNATURE_PATTERN,
            "Authority binding signature",
        ),
    } as DeviceAuthorization["binding"];
    if (binding.protocol !== "cgp/device-authority/1") {
        throw new Error("Device authority protocol is unsupported");
    }

    const label =
        typeof value.certificate?.label === "string"
            ? value.certificate.label.trim().replace(/\s+/g, " ")
            : "";
    if (!label || label.length > 64) {
        throw new Error("Device label is invalid");
    }
    const certificate = {
        protocol: value.certificate?.protocol,
        accountPublicKey: normalizedHex(
            value.certificate?.accountPublicKey,
            PUBLIC_KEY_PATTERN,
            "Certificate account public key",
        ),
        authorityPublicKey: normalizedHex(
            value.certificate?.authorityPublicKey,
            PUBLIC_KEY_PATTERN,
            "Certificate authority public key",
        ),
        devicePublicKey: normalizedHex(
            value.certificate?.devicePublicKey,
            PUBLIC_KEY_PATTERN,
            "Device public key",
        ),
        serial: normalizedHex(
            value.certificate?.serial,
            SERIAL_PATTERN,
            "Device certificate serial",
        ),
        label,
        capabilities: normalizeCapabilities(value.certificate?.capabilities),
        issuedAt: normalizedInteger(
            value.certificate?.issuedAt,
            "Certificate issue time",
            1,
        ),
        expiresAt: normalizedInteger(
            value.certificate?.expiresAt,
            "Certificate expiry",
            1,
        ),
        signature: normalizedHex(
            value.certificate?.signature,
            SIGNATURE_PATTERN,
            "Device certificate signature",
        ),
    } as DeviceCertificate;
    if (
        certificate.protocol !== "cgp/device-certificate/1" ||
        certificate.expiresAt <= certificate.issuedAt
    ) {
        throw new Error("Device certificate lifetime is invalid");
    }

    if (
        !Array.isArray(value.revocation?.revokedSerials) ||
        value.revocation.revokedSerials.length > MAX_REVOKED_CERTIFICATES
    ) {
        throw new Error("Device revocation list is invalid");
    }
    const revokedSerials = Array.from(
        new Set(
            value.revocation.revokedSerials.map((serial) =>
                normalizedHex(serial, SERIAL_PATTERN, "Revoked certificate serial"),
            ),
        ),
    ).sort();
    const revocation = {
        protocol: value.revocation?.protocol,
        accountPublicKey: normalizedHex(
            value.revocation?.accountPublicKey,
            PUBLIC_KEY_PATTERN,
            "Revocation account public key",
        ),
        authorityPublicKey: normalizedHex(
            value.revocation?.authorityPublicKey,
            PUBLIC_KEY_PATTERN,
            "Revocation authority public key",
        ),
        generation: normalizedInteger(
            value.revocation?.generation,
            "Revocation generation",
            1,
        ),
        epoch: normalizedInteger(value.revocation?.epoch, "Revocation epoch"),
        updatedAt: normalizedInteger(
            value.revocation?.updatedAt,
            "Revocation timestamp",
            1,
        ),
        revokedSerials,
        signature: normalizedHex(
            value.revocation?.signature,
            SIGNATURE_PATTERN,
            "Revocation signature",
        ),
    } as DeviceRevocationState;
    if (revocation.protocol !== "cgp/device-revocation/1") {
        throw new Error("Device revocation protocol is unsupported");
    }

    return {
        protocol: "cgp/device-authorization/1",
        binding,
        certificate,
        revocation,
    };
}

export function verifyDeviceAuthorizedObject(
    payload: unknown,
    signatureValue: string,
    authorizationValue: DeviceAuthorization,
    options: {
        accountPublicKey: string;
        requiredCapability?: DeviceCapability;
        minimumRevocationEpoch?: number;
        now?: number;
    },
): DeviceAuthorizationVerification {
    try {
        const now = options.now ?? Date.now();
        const accountPublicKey = normalizedHex(
            options.accountPublicKey,
            PUBLIC_KEY_PATTERN,
            "Expected account public key",
        );
        const signature = normalizedHex(
            signatureValue,
            SIGNATURE_PATTERN,
            "Device payload signature",
        );
        const authorization = normalizeAuthorization(authorizationValue);
        const { binding, certificate, revocation } = authorization;
        if (
            binding.accountPublicKey !== accountPublicKey ||
            certificate.accountPublicKey !== accountPublicKey ||
            revocation.accountPublicKey !== accountPublicKey ||
            certificate.authorityPublicKey !== binding.authorityPublicKey ||
            revocation.authorityPublicKey !== binding.authorityPublicKey ||
            revocation.generation !== binding.generation
        ) {
            throw new Error("Device authorization has inconsistent account authority");
        }
        if (
            binding.activatedAt > now + MAX_CLOCK_SKEW_MS ||
            certificate.issuedAt < binding.activatedAt ||
            certificate.issuedAt > now + MAX_CLOCK_SKEW_MS ||
            certificate.expiresAt <= now ||
            revocation.updatedAt < binding.activatedAt ||
            revocation.updatedAt > now + MAX_CLOCK_SKEW_MS
        ) {
            throw new Error("Device authorization is expired or not yet valid");
        }
        if (revocation.epoch < (options.minimumRevocationEpoch ?? 0)) {
            throw new Error("Device authorization uses a stale revocation epoch");
        }
        if (revocation.revokedSerials.includes(certificate.serial)) {
            throw new Error("Device certificate has been revoked");
        }
        if (
            options.requiredCapability &&
            !certificate.capabilities.includes(options.requiredCapability)
        ) {
            throw new Error(
                `Device certificate does not allow ${options.requiredCapability}`,
            );
        }

        const { signature: bindingSignature, ...unsignedBinding } = binding;
        const { signature: certificateSignature, ...unsignedCertificate } =
            certificate;
        const { signature: revocationSignature, ...unsignedRevocation } =
            revocation;
        if (
            !verifyObject(
                binding.accountPublicKey,
                unsignedBinding,
                bindingSignature,
            ) ||
            !verifyObject(
                certificate.authorityPublicKey,
                unsignedCertificate,
                certificateSignature,
            ) ||
            !verifyObject(
                revocation.authorityPublicKey,
                unsignedRevocation,
                revocationSignature,
            ) ||
            !verifyObject(
                certificate.devicePublicKey,
                { payload, deviceAuthorization: authorization },
                signature,
            )
        ) {
            throw new Error("Device authorization signature verification failed");
        }
        return {
            ok: true,
            accountPublicKey,
            authorityPublicKey: binding.authorityPublicKey,
            devicePublicKey: certificate.devicePublicKey,
            revocationEpoch: revocation.epoch,
        };
    } catch (error) {
        return {
            ok: false,
            error:
                error instanceof Error
                    ? error.message
                    : "Device authorization failed",
        };
    }
}

export class DeviceAuthorityRegistry {
    private pins = new Map<string, DeviceAuthorityPin>();

    constructor(private readonly maxAccounts = 100_000) {}

    get(accountPublicKey: string) {
        return this.pins.get(accountPublicKey.toLowerCase());
    }

    verify(
        payload: unknown,
        signature: string,
        accountPublicKey: string,
        authorization: DeviceAuthorization | undefined,
        requiredCapability: DeviceCapability,
        now = Date.now(),
    ): DeviceAuthorizationVerification {
        if (
            typeof accountPublicKey !== "string" ||
            !PUBLIC_KEY_PATTERN.test(accountPublicKey.trim().toLowerCase()) ||
            typeof signature !== "string"
        ) {
            return {
                ok: false,
                error: "Account authorization fields are invalid",
            };
        }
        const account = accountPublicKey.toLowerCase();
        const current = this.pins.get(account);
        if (!authorization) {
            if (current) {
                return {
                    ok: false,
                    error: "Direct account signatures are disabled after device authority activation",
                };
            }
            return verifyObject(account, payload, signature)
                ? { ok: true, accountPublicKey: account }
                : { ok: false, error: "Account signature verification failed" };
        }
        if (
            current &&
            (authorization?.binding?.authorityPublicKey?.toLowerCase() !==
                current.authorityPublicKey ||
                authorization?.binding?.generation !== current.generation)
        ) {
            return {
                ok: false,
                error: "Device authorization conflicts with the pinned account authority",
            };
        }
        const verified = verifyDeviceAuthorizedObject(
            payload,
            signature,
            authorization,
            {
                accountPublicKey: account,
                requiredCapability,
                minimumRevocationEpoch: current?.revocationEpoch,
                now,
            },
        );
        if (!verified.ok || !verified.authorityPublicKey) {
            return verified;
        }
        this.remember({
            accountPublicKey: account,
            authorityPublicKey: verified.authorityPublicKey,
            generation: authorization.binding.generation,
            activatedAt: authorization.binding.activatedAt,
            revocationEpoch: verified.revocationEpoch ?? 0,
            observedAt: now,
        });
        return verified;
    }

    private remember(pin: DeviceAuthorityPin) {
        this.pins.delete(pin.accountPublicKey);
        this.pins.set(pin.accountPublicKey, pin);
        while (this.pins.size > this.maxAccounts) {
            const oldest = this.pins.keys().next().value;
            if (!oldest) break;
            this.pins.delete(oldest);
        }
    }
}
