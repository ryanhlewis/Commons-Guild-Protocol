import {verifyArchiveEvent} from "../../../scripts/backup-validation";
import { describe, expect, it } from "vitest";
import {
    DeviceAuthorityRegistry,
    createRelayWriteCertificate, computeEventId, relayWriteProposalId, verifyRelayWriteCertificate,
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

it.each([100,7200000])("verifies historical certificate time boundary %i",async(offset)=>{
 const value=await fixture();const createdAt=NOW+offset,body={type:"GUILD_CREATE" as const,guildId:"historical-cert",name:"History"};
 const author=value.accountPublicKey,payload={body,author,createdAt};
 const event:any={...payload,seq:0,prevHash:null,deviceAuthorization:value.authorization,signature:await value.signPayload(payload)};event.id=computeEventId(event);
 const keys=[generatePrivateKey(),generatePrivateKey(),generatePrivateKey()];const policy={epoch:"history",members:keys.map(getPublicKey),requiredVotes:2};
 const proposal={guildId:body.guildId,headSeq:-1,headHash:null,...payload,signature:event.signature,deviceAuthorization:event.deviceAuthorization};const proposalId=relayWriteProposalId(policy.epoch,proposal);
 const votes=await Promise.all(keys.slice(0,2).map(async key=>{const unsigned={protocol:"cgp/write-vote/1" as const,epoch:policy.epoch,relayPublicKey:getPublicKey(key),guildId:body.guildId,headSeq:-1,headHash:null,proposalId,votedAt:createdAt};return {...unsigned,signature:await sign(key,hashObject(unsigned))};}));
 event.writeCertificate=createRelayWriteCertificate(policy,proposal,votes);
 expect(verifyDeviceAuthorizedObject(payload,event.signature,value.authorization,{accountPublicKey:author,requiredCapability:"publish",now:NOW+7200000}).ok).toBe(false);
 expect(verifyRelayWriteCertificate(event)).toBe(offset===100);
 expect(verifyArchiveEvent(event).length===0).toBe(offset===100);
});
