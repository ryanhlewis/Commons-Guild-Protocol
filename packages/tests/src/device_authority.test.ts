import { describe, expect, it } from "vitest";
import {
    DeviceAuthorityRegistry,
    encodeCgpFrame,
    generatePrivateKey,
    getPublicKey,
    hashObject,
    parseCgpWireData,
    sign,
    verifyDeviceAuthorizedObject,
    type DeviceAuthorization,
    type DeviceRevocationState,
} from "@cgp/core";

const NOW = 2_000_000_000_000;

async function fixture() {
    const accountPrivateKey = generatePrivateKey();
    const authorityPrivateKey = generatePrivateKey();
    const devicePrivateKey = generatePrivateKey();
    const accountPublicKey = getPublicKey(accountPrivateKey);
    const authorityPublicKey = getPublicKey(authorityPrivateKey);
    const devicePublicKey = getPublicKey(devicePrivateKey);
    const unsignedBinding = {
        protocol: "cgp/device-authority/1" as const,
        accountPublicKey,
        authorityPublicKey,
        generation: 1,
        activatedAt: NOW,
    };
    const binding = {
        ...unsignedBinding,
        signature: await sign(accountPrivateKey, hashObject(unsignedBinding)),
    };
    const certificateUnsigned = {
        protocol: "cgp/device-certificate/1" as const,
        accountPublicKey,
        authorityPublicKey,
        devicePublicKey,
        serial: "11".repeat(16),
        label: "Test device",
        capabilities: ["device-link", "mls", "publish", "read"] as const,
        issuedAt: NOW,
        expiresAt: NOW + 60 * 60 * 1000,
    };
    const certificate = {
        ...certificateUnsigned,
        capabilities: [...certificateUnsigned.capabilities],
        signature: await sign(
            authorityPrivateKey,
            hashObject(certificateUnsigned),
        ),
    };
    const createRevocation = async (
        epoch: number,
        revokedSerials: string[] = [],
    ): Promise<DeviceRevocationState> => {
        const unsigned = {
            protocol: "cgp/device-revocation/1" as const,
            accountPublicKey,
            authorityPublicKey,
            generation: 1,
            epoch,
            updatedAt: NOW + epoch,
            revokedSerials: [...revokedSerials].sort(),
        };
        return {
            ...unsigned,
            signature: await sign(
                authorityPrivateKey,
                hashObject(unsigned),
            ),
        };
    };
    const authorization = {
        protocol: "cgp/device-authorization/1" as const,
        binding,
        certificate,
        revocation: await createRevocation(0),
    } satisfies DeviceAuthorization;
    const signPayload = (payload: unknown, auth = authorization) =>
        sign(
            devicePrivateKey,
            hashObject({ payload, deviceAuthorization: auth }),
        );
    return {
        accountPrivateKey,
        accountPublicKey,
        authorization,
        createRevocation,
        signPayload,
    };
}

describe("delegated CGP device authorization", () => {
    it("verifies account binding, device certificate, and payload together", async () => {
        const value = await fixture();
        const payload = {
            body: { type: "MESSAGE", guildId: "guild", content: "hello" },
            author: value.accountPublicKey,
            createdAt: NOW,
        };
        const signature = await value.signPayload(payload);

        expect(
            verifyDeviceAuthorizedObject(
                payload,
                signature,
                value.authorization,
                {
                    accountPublicKey: value.accountPublicKey,
                    requiredCapability: "publish",
                    now: NOW + 1,
                },
            ),
        ).toMatchObject({
            ok: true,
            accountPublicKey: value.accountPublicKey,
            devicePublicKey:
                value.authorization.certificate.devicePublicKey,
        });
    });

    it("pins an authority and rejects direct-root and stale device frames", async () => {
        const value = await fixture();
        const registry = new DeviceAuthorityRegistry();
        const payload = {
            body: { type: "MESSAGE", guildId: "guild", content: "new" },
            author: value.accountPublicKey,
            createdAt: NOW,
        };
        const newerAuthorization = {
            ...value.authorization,
            revocation: await value.createRevocation(2),
        };
        const newerSignature = await value.signPayload(
            payload,
            newerAuthorization,
        );
        expect(
            registry.verify(
                payload,
                newerSignature,
                value.accountPublicKey,
                newerAuthorization,
                "publish",
                NOW + 3,
            ).ok,
        ).toBe(true);

        const rootSignature = await sign(
            value.accountPrivateKey,
            hashObject(payload),
        );
        expect(
            registry.verify(
                payload,
                rootSignature,
                value.accountPublicKey,
                undefined,
                "publish",
                NOW + 3,
            ),
        ).toMatchObject({ ok: false });

        const staleSignature = await value.signPayload(payload);
        expect(
            registry.verify(
                payload,
                staleSignature,
                value.accountPublicKey,
                value.authorization,
                "publish",
                NOW + 3,
            ),
        ).toMatchObject({
            ok: false,
            error: "Device authorization uses a stale revocation epoch",
        });
    });

    it("preserves device authorization in binary-v2 publish frames", async () => {
        const value = await fixture();
        const payload = {
            body: { type: "MESSAGE", guildId: "guild", content: "hello" },
            author: value.accountPublicKey,
            signature: "22".repeat(64),
            deviceAuthorization: value.authorization,
            createdAt: NOW,
            clientEventId: "client-1",
        };
        const encoded = encodeCgpFrame("PUBLISH", payload, "binary-v2");
        expect(parseCgpWireData(encoded).payload).toEqual(payload);
    });
});
