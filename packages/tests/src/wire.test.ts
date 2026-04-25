import { describe, expect, it } from "vitest";
import {
    encodeCgpPubSubFrame,
    InvalidCgpFrameError,
    encodeCgpFrame,
    encodeCgpWireFrame,
    parseCgpPubSubData,
    parseCgpFrame,
    parseCgpWireData,
    rawDataToUtf8String,
    socketDataToCgpFrame,
    socketDataToUtf8String
} from "@cgp/core";

describe("core wire helpers", () => {
    it("parses a valid frame payload", () => {
        const frame = parseCgpFrame(`["PING",{"ok":true}]`);
        expect(frame.kind).toBe("PING");
        expect(frame.payload).toEqual({ ok: true });
    });

    it("rejects malformed frames", () => {
        expect(() => parseCgpFrame("not-json")).toThrowError(InvalidCgpFrameError);
        expect(() => parseCgpFrame(`{"kind":"PING"}`)).toThrowError(InvalidCgpFrameError);
        expect(() => parseCgpFrame(`[123,{}]`)).toThrowError(InvalidCgpFrameError);
    });

    it("decodes raw utf8 data in sync contexts", () => {
        expect(rawDataToUtf8String("hello")).toBe("hello");
        expect(rawDataToUtf8String(Buffer.from("relay", "utf8"))).toBe("relay");
        expect(rawDataToUtf8String([Buffer.from("re"), Buffer.from("lay")])).toBe("relay");
        expect(rawDataToUtf8String(new Uint8Array([104, 105]))).toBe("hi");
    });

    it("decodes socket data including blobs", async () => {
        const bytes = new Uint8Array([119, 115]);
        expect(await socketDataToUtf8String(bytes.buffer)).toBe("ws");
        expect(await socketDataToUtf8String(bytes)).toBe("ws");
        if (typeof Blob !== "undefined") {
            expect(await socketDataToUtf8String(new Blob(["blob"]))).toBe("blob");
        }
    });

    it("encodes binary-json wire frames as bytes", () => {
        const encoded = encodeCgpWireFrame(`["HELLO",{}]`, "binary-json");
        expect(encoded).toBeInstanceOf(Uint8Array);
        expect(Buffer.from(encoded as Uint8Array).toString("utf8")).toBe(`["HELLO",{}]`);
        expect(encodeCgpWireFrame(`["HELLO",{}]`, "json")).toBe(`["HELLO",{}]`);
    });

    it("encodes and parses compact binary-v1 frames", async () => {
        const encoded = encodeCgpFrame("HELLO", { protocol: "cgp/0.1", wireFormat: "binary-v1" }, "binary-v1");
        expect(encoded).toBeInstanceOf(Uint8Array);
        const parsed = parseCgpWireData(encoded);
        expect(parsed.kind).toBe("HELLO");
        expect(parsed.payload).toEqual({ protocol: "cgp/0.1", wireFormat: "binary-v1" });
        expect(parsed.rawFrame).toBeUndefined();
        expect(parseCgpWireData(encoded, { includeRawFrame: true }).rawFrame).toBe(`["HELLO",{"protocol":"cgp/0.1","wireFormat":"binary-v1"}]`);
        await expect(socketDataToCgpFrame(encoded)).resolves.toMatchObject({
            kind: "HELLO",
            payload: { protocol: "cgp/0.1", wireFormat: "binary-v1" },
            rawFrame: `["HELLO",{"protocol":"cgp/0.1","wireFormat":"binary-v1"}]`
        });
    });

    it("lets pre-stringified frames use compact binary-v1 compatibility encoding", () => {
        const encoded = encodeCgpWireFrame(`["PING",{"ok":true}]`, "binary-v1");
        const parsed = parseCgpWireData(encoded);
        expect(parsed.kind).toBe("PING");
        expect(parsed.payload).toEqual({ ok: true });
    });

    it("encodes compact binary-v2 publish frames", () => {
        const payload = {
            body: { type: "MESSAGE", guildId: "g", channelId: "c", content: "hello" },
            author: "author-key",
            signature: "signature",
            createdAt: 1234,
            clientEventId: "client-1"
        };
        const encoded = encodeCgpFrame("PUBLISH", payload, "binary-v2");
        expect(encoded).toBeInstanceOf(Uint8Array);
        expect((encoded as Uint8Array)[3]).toBe(0x02);
        const parsed = parseCgpWireData(encoded, { includeRawFrame: true });
        expect(parsed.kind).toBe("PUBLISH");
        expect(parsed.payload).toEqual(payload);
        expect(parsed.rawFrame).toBe(`["PUBLISH",${JSON.stringify(payload)}]`);
    });

    it("encodes compact binary-v2 publish batch frames", () => {
        const event = (index: number) => ({
            body: { type: "MESSAGE", guildId: "g", channelId: "c", content: `hello-${index}` },
            author: "author-key",
            signature: `signature-${index}`,
            createdAt: 1234 + index,
            clientEventId: `client-${index}`
        });
        const payload = { batchId: "batch-1", events: [event(1), event(2)] };
        const encoded = encodeCgpFrame("PUBLISH_BATCH", payload, "binary-v2");
        expect(encoded).toBeInstanceOf(Uint8Array);
        expect((encoded as Uint8Array)[3]).toBe(0x02);
        const parsed = parseCgpWireData(encoded, { includeRawFrame: true });
        expect(parsed.kind).toBe("PUBLISH_BATCH");
        expect(parsed.payload).toEqual(payload);
        expect(parsed.rawFrame).toBe(`["PUBLISH_BATCH",${JSON.stringify(payload)}]`);
    });

    it("falls back to generic binary-v2 frames for non-hot frame kinds", () => {
        const encoded = encodeCgpFrame("PING", { ok: true }, "binary-v2");
        expect(encoded).toBeInstanceOf(Uint8Array);
        const parsed = parseCgpWireData(encoded);
        expect(parsed.kind).toBe("PING");
        expect(parsed.payload).toEqual({ ok: true });
    });

    it("rejects malformed compact binary-v1 frames", () => {
        expect(() => parseCgpWireData(new Uint8Array([0x43, 0x47, 0x50, 0x01, 0]))).toThrowError(InvalidCgpFrameError);
        expect(() => parseCgpWireData(new Uint8Array([0x43, 0x47, 0x50, 0x01, 4, 0x50, 0x49]))).toThrowError(InvalidCgpFrameError);
    });

    it("encodes and parses compact binary-v1 pubsub frames without generic frame wrapping", () => {
        const encoded = encodeCgpPubSubFrame(
            "PUB",
            {
                topic: "guild:alpha:log",
                envelope: {
                    originId: "relay-a",
                    guildId: "guild-alpha",
                    events: [{ seq: 42, body: { type: "MESSAGE" } }]
                }
            },
            "binary-v1"
        );
        expect(encoded).toBeInstanceOf(Uint8Array);
        const parsed = parseCgpPubSubData(encoded, { includeRawFrame: true });
        expect(parsed.kind).toBe("PUB");
        expect(parsed.payload).toEqual({
            topic: "guild:alpha:log",
            envelope: {
                originId: "relay-a",
                guildId: "alpha",
                events: [{ seq: 42, body: { type: "MESSAGE" } }]
            }
        });
        expect(parsed.rawFrame).toBe(
            `["PUB",{"topic":"guild:alpha:log","envelope":{"originId":"relay-a","guildId":"alpha","events":[{"seq":42,"body":{"type":"MESSAGE"}}]}}]`
        );
    });

    it("preserves compact binary-v1 pubsub live topic hints", () => {
        const encoded = encodeCgpPubSubFrame(
            "PUB",
            {
                topic: "guild:alpha:log",
                envelope: {
                    originId: "relay-a",
                    guildId: "guild-alpha",
                    events: [{ seq: 43, body: { type: "MESSAGE" } }],
                    liveTopics: false
                }
            },
            "binary-v1"
        );
        const parsed = parseCgpPubSubData(encoded);
        expect(parsed.payload).toEqual({
            topic: "guild:alpha:log",
            envelope: {
                originId: "relay-a",
                guildId: "alpha",
                events: [{ seq: 43, body: { type: "MESSAGE" } }],
                liveTopics: false
            }
        });
    });
});
