import { afterAll, beforeAll, describe, expect, it } from "vitest";
import WebSocket from "ws";
import {
    generatePrivateKey,
    getPublicKey,
    hashObject,
    sign,
    type DeviceAuthorization,
} from "@cgp/core";
import { RelayServer } from "@cgp/relay/src/server";
import { MemoryStore } from "@cgp/relay/src/store";
import { CgpClient } from "@cgp/client";

async function deviceFixture(now = Date.now()) {
    const accountPrivateKey = generatePrivateKey();
    const authorityPrivateKey = generatePrivateKey();
    const devicePrivateKey = generatePrivateKey();
    const accountPublicKey = getPublicKey(accountPrivateKey);
    const authorityPublicKey = getPublicKey(authorityPrivateKey);
    const devicePublicKey = getPublicKey(devicePrivateKey);
    const bindingUnsigned = {
        protocol: "cgp/device-authority/1" as const,
        accountPublicKey,
        authorityPublicKey,
        generation: 1,
        activatedAt: now,
    };
    const binding = {
        ...bindingUnsigned,
        signature: await sign(accountPrivateKey, hashObject(bindingUnsigned)),
    };
    const certificateUnsigned = {
        protocol: "cgp/device-certificate/1" as const,
        accountPublicKey,
        authorityPublicKey,
        devicePublicKey,
        serial: "33".repeat(16),
        label: "Relay integration device",
        capabilities: ["device-link", "mls", "publish", "read"] as const,
        issuedAt: now,
        expiresAt: now + 60 * 60 * 1000,
    };
    const certificate = {
        ...certificateUnsigned,
        capabilities: [...certificateUnsigned.capabilities],
        signature: await sign(
            authorityPrivateKey,
            hashObject(certificateUnsigned),
        ),
    };
    const revocationUnsigned = {
        protocol: "cgp/device-revocation/1" as const,
        accountPublicKey,
        authorityPublicKey,
        generation: 1,
        epoch: 0,
        updatedAt: now,
        revokedSerials: [],
    };
    const authorization = {
        protocol: "cgp/device-authorization/1" as const,
        binding,
        certificate,
        revocation: {
            ...revocationUnsigned,
            signature: await sign(
                authorityPrivateKey,
                hashObject(revocationUnsigned),
            ),
        },
    } satisfies DeviceAuthorization;
    return {
        accountPrivateKey,
        accountPublicKey,
        devicePrivateKey,
        authorization,
        signDevice: (payload: unknown) =>
            sign(
                devicePrivateKey,
                hashObject({ payload, deviceAuthorization: authorization }),
            ),
    };
}

describe("relay delegated device authorization", () => {
    let relay: RelayServer;
    let store: MemoryStore;
    let relayUrl: string;

    beforeAll(async () => {
        store = new MemoryStore();
        relay = new RelayServer(0, store, [], {
            enableDefaultPlugins: false,
        });
        const startedAt = Date.now();
        while (!Number.isFinite(relay.getPort())) {
            if (Date.now() - startedAt > 5000) {
                throw new Error("Relay did not start");
            }
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
        relayUrl = `ws://localhost:${relay.getPort()}`;
    });

    afterAll(async () => {
        await relay.close();
    });

    it("persists delegated frames and fences direct-root fallback after activation", async () => {
        const device = await deviceFixture();
        const socket = new WebSocket(relayUrl);
        await new Promise<void>((resolve, reject) => {
            socket.once("open", resolve);
            socket.once("error", reject);
        });
        const responses: Array<[string, any]> = [];
        socket.on("message", (raw) => {
            responses.push(JSON.parse(raw.toString()));
        });
        const waitFor = async (clientEventId: string) => {
            const startedAt = Date.now();
            while (Date.now() - startedAt < 5000) {
                const match = responses.find(
                    ([, payload]) => payload?.clientEventId === clientEventId,
                );
                if (match) return match;
                await new Promise((resolve) => setTimeout(resolve, 10));
            }
            throw new Error(`Timed out waiting for ${clientEventId}`);
        };
        const publishDevice = async (
            body: Record<string, unknown>,
            clientEventId: string,
        ) => {
            const createdAt = Date.now();
            const unsigned = {
                body,
                author: device.accountPublicKey,
                createdAt,
            };
            socket.send(
                JSON.stringify([
                    "PUBLISH",
                    {
                        ...unsigned,
                        signature: await device.signDevice(unsigned),
                        deviceAuthorization: device.authorization,
                        clientEventId,
                    },
                ]),
            );
            return await waitFor(clientEventId);
        };

        try {
            const guildId = hashObject({
                type: "device-authority-relay-test",
                account: device.accountPublicKey,
            });
            expect(
                await publishDevice(
                    {
                        type: "GUILD_CREATE",
                        guildId,
                        name: "Delegated Guild",
                        access: "public",
                    },
                    "delegated-genesis",
                ),
            ).toMatchObject(["PUB_ACK", { seq: 0 }]);

            const rootBody = {
                type: "CHANNEL_CREATE",
                guildId,
                channelId: "root-channel",
                name: "root-channel",
                kind: "text",
            };
            const rootCreatedAt = Date.now();
            socket.send(
                JSON.stringify([
                    "PUBLISH",
                    {
                        body: rootBody,
                        author: device.accountPublicKey,
                        createdAt: rootCreatedAt,
                        signature: await sign(
                            device.accountPrivateKey,
                            hashObject({
                                body: rootBody,
                                author: device.accountPublicKey,
                                createdAt: rootCreatedAt,
                            }),
                        ),
                        clientEventId: "root-after-activation",
                    },
                ]),
            );
            expect(await waitFor("root-after-activation")).toMatchObject([
                "ERROR",
                {
                    code: "INVALID_SIGNATURE",
                    message:
                        "Direct account signatures are disabled after device authority activation",
                },
            ]);

            expect(
                await publishDevice(
                    {
                        type: "CHANNEL_CREATE",
                        guildId,
                        channelId: "device-channel",
                        name: "device-channel",
                        kind: "text",
                    },
                    "delegated-channel",
                ),
            ).toMatchObject(["PUB_ACK", { seq: 1 }]);

            const log = store.getLog(guildId);
            expect(log).toHaveLength(2);
            expect(log[1].deviceAuthorization).toEqual(
                device.authorization,
            );
        } finally {
            socket.close();
        }
    });

    it("supports delegated reads and writes through the generic CGP client", async () => {
        const device = await deviceFixture();
        const client = new CgpClient({
            relays: [relayUrl],
            keyPair: {
                pub: device.accountPublicKey,
                priv: device.accountPrivateKey,
            },
            deviceSigner: {
                privateKey: device.devicePrivateKey,
                authorization: device.authorization,
            },
        });
        try {
            await client.connect();
            const guildId = await client.createGuild("Device Client Guild");
            const channelId = await client.createChannel(
                guildId,
                "general",
                "text",
            );
            const startedAt = Date.now();
            while (
                store.getLog(guildId).length < 2 &&
                Date.now() - startedAt < 5000
            ) {
                await new Promise((resolve) => setTimeout(resolve, 10));
            }
            expect(channelId).toBeTruthy();
            expect(store.getLog(guildId)).toHaveLength(2);
            expect(
                store
                    .getLog(guildId)
                    .every(
                        (event) =>
                            event.deviceAuthorization?.certificate.serial ===
                            device.authorization.certificate.serial,
                    ),
            ).toBe(true);
        } finally {
            await client.close();
        }
    });

    it("rebuilds the direct-root fence from durable history after restart", async () => {
        const restartStore = new MemoryStore();
        const firstRelay = new RelayServer(0, restartStore, [], {
            enableDefaultPlugins: false,
        });
        while (!Number.isFinite(firstRelay.getPort())) {
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
        const device = await deviceFixture();
        const client = new CgpClient({
            relays: [`ws://localhost:${firstRelay.getPort()}`],
            keyPair: {
                pub: device.accountPublicKey,
                priv: device.accountPrivateKey,
            },
            deviceSigner: {
                privateKey: device.devicePrivateKey,
                authorization: device.authorization,
            },
        });
        const guildId = hashObject({
            type: "restart-device-authority-test",
            account: device.accountPublicKey,
        });
        await client.connect();
        await client.publishReliable({
            type: "GUILD_CREATE",
            guildId,
            name: "Restart Guild",
            access: "public",
        });
        await client.close();
        await firstRelay.close();

        const restartedRelay = new RelayServer(0, restartStore, [], {
            enableDefaultPlugins: false,
        });
        while (!Number.isFinite(restartedRelay.getPort())) {
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
        const socket = new WebSocket(
            `ws://localhost:${restartedRelay.getPort()}`,
        );
        await new Promise<void>((resolve, reject) => {
            socket.once("open", resolve);
            socket.once("error", reject);
        });
        try {
            const body = {
                type: "CHANNEL_CREATE",
                guildId,
                channelId: "root-after-restart",
                name: "root-after-restart",
                kind: "text",
            };
            const createdAt = Date.now();
            const clientEventId = "root-after-restart";
            const response = new Promise<[string, any]>((resolve, reject) => {
                const timeout = setTimeout(
                    () => reject(new Error("Timed out waiting for restart fence")),
                    5000,
                );
                socket.on("message", (raw) => {
                    const frame = JSON.parse(raw.toString()) as [string, any];
                    if (frame[1]?.clientEventId !== clientEventId) return;
                    clearTimeout(timeout);
                    resolve(frame);
                });
            });
            socket.send(
                JSON.stringify([
                    "PUBLISH",
                    {
                        body,
                        author: device.accountPublicKey,
                        createdAt,
                        signature: await sign(
                            device.accountPrivateKey,
                            hashObject({
                                body,
                                author: device.accountPublicKey,
                                createdAt,
                            }),
                        ),
                        clientEventId,
                    },
                ]),
            );
            expect(await response).toMatchObject([
                "ERROR",
                {
                    code: "INVALID_SIGNATURE",
                    message:
                        "Direct account signatures are disabled after device authority activation",
                },
            ]);
        } finally {
            socket.close();
            await restartedRelay.close();
        }
    });
});
