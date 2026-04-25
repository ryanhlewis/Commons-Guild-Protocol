import { performance } from "node:perf_hooks";
import { encodeCgpFrame, encodeCgpWireFrame, parseCgpWireData, encodeCgpPubSubFrame, parseCgpPubSubData } from "@cgp/core";

interface BenchCase {
    name: string;
    bytes?: number;
    run: () => void;
}

function sampleEvent(seq: number) {
    return {
        id: `evt-${seq}`,
        guildId: "alpha",
        seq,
        prevHash: seq > 1 ? `hash-${seq - 1}` : "",
        author: "relay-a",
        createdAt: 1_700_000_000_000 + seq,
        body: {
            type: "MESSAGE",
            channelId: "general",
            content: `hello ${seq}`,
            attachments: []
        },
        sig: "sig"
    };
}

const eventEnvelope = {
    topic: "guild:alpha:log",
    envelope: {
        originId: "relay-a",
        guildId: "alpha",
        event: sampleEvent(42)
    }
};

const batchEnvelope = {
    topic: "guild:alpha:log",
    envelope: {
        originId: "relay-a",
        guildId: "alpha",
        events: Array.from({ length: 32 }, (_, index) => sampleEvent(index + 1))
    }
};

const publishBatchPayload = {
    batchId: "bench-batch",
    events: Array.from({ length: 250 }, (_, index) => ({
        body: {
            type: "MESSAGE",
            guildId: "alpha",
            channelId: "general",
            messageId: `msg-${index}`,
            content: `hello ${index}`
        },
        author: "relay-a",
        signature: "sig",
        createdAt: 1_700_000_000_000 + index,
        clientEventId: `bench-batch:${index}`
    }))
};

function bench(caseInfo: BenchCase, iterations: number) {
    for (let index = 0; index < Math.min(iterations, 5_000); index += 1) {
        caseInfo.run();
    }
    const start = performance.now();
    for (let index = 0; index < iterations; index += 1) {
        caseInfo.run();
    }
    const elapsedMs = performance.now() - start;
    const opsPerSec = iterations / (elapsedMs / 1000);
    return {
        name: caseInfo.name,
        iterations,
        elapsedMs: Number(elapsedMs.toFixed(2)),
        opsPerSec: Number(opsPerSec.toFixed(2)),
        bytes: caseInfo.bytes
    };
}

const genericEventFrame = encodeCgpFrame("PUB", eventEnvelope, "binary-v1") as Uint8Array;
const compactEventFrame = encodeCgpPubSubFrame("PUB", eventEnvelope, "binary-v1") as Uint8Array;
const genericBatchFrame = encodeCgpFrame("PUB", batchEnvelope, "binary-v1") as Uint8Array;
const compactBatchFrame = encodeCgpPubSubFrame("PUB", batchEnvelope, "binary-v1") as Uint8Array;
const publishBatchRawFrame = encodeCgpWireFrame(JSON.stringify(["PUBLISH_BATCH", publishBatchPayload]), "binary-v1") as Uint8Array;
const publishBatchDirectFrame = encodeCgpFrame("PUBLISH_BATCH", publishBatchPayload, "binary-v1") as Uint8Array;
const publishBatchV2Frame = encodeCgpFrame("PUBLISH_BATCH", publishBatchPayload, "binary-v2") as Uint8Array;

const cases: BenchCase[] = [
    {
        name: "encode:generic:event",
        bytes: genericEventFrame.byteLength,
        run: () => {
            encodeCgpFrame("PUB", eventEnvelope, "binary-v1");
        }
    },
    {
        name: "encode:compact:event",
        bytes: compactEventFrame.byteLength,
        run: () => {
            encodeCgpPubSubFrame("PUB", eventEnvelope, "binary-v1");
        }
    },
    {
        name: "parse:generic:event",
        bytes: genericEventFrame.byteLength,
        run: () => {
            parseCgpWireData(genericEventFrame, { includeRawFrame: false });
        }
    },
    {
        name: "parse:compact:event",
        bytes: compactEventFrame.byteLength,
        run: () => {
            parseCgpPubSubData(compactEventFrame, { includeRawFrame: false });
        }
    },
    {
        name: "encode:generic:batch32",
        bytes: genericBatchFrame.byteLength,
        run: () => {
            encodeCgpFrame("PUB", batchEnvelope, "binary-v1");
        }
    },
    {
        name: "encode:compact:batch32",
        bytes: compactBatchFrame.byteLength,
        run: () => {
            encodeCgpPubSubFrame("PUB", batchEnvelope, "binary-v1");
        }
    },
    {
        name: "parse:generic:batch32",
        bytes: genericBatchFrame.byteLength,
        run: () => {
            parseCgpWireData(genericBatchFrame, { includeRawFrame: false });
        }
    },
    {
        name: "parse:compact:batch32",
        bytes: compactBatchFrame.byteLength,
        run: () => {
            parseCgpPubSubData(compactBatchFrame, { includeRawFrame: false });
        }
    },
    {
        name: "encode:publish-batch:raw-json",
        bytes: publishBatchRawFrame.byteLength,
        run: () => {
            encodeCgpWireFrame(JSON.stringify(["PUBLISH_BATCH", publishBatchPayload]), "binary-v1");
        }
    },
    {
        name: "encode:publish-batch:direct",
        bytes: publishBatchDirectFrame.byteLength,
        run: () => {
            encodeCgpFrame("PUBLISH_BATCH", publishBatchPayload, "binary-v1");
        }
    },
    {
        name: "encode:publish-batch:binary-v2",
        bytes: publishBatchV2Frame.byteLength,
        run: () => {
            encodeCgpFrame("PUBLISH_BATCH", publishBatchPayload, "binary-v2");
        }
    },
    {
        name: "parse:publish-batch:binary-v2",
        bytes: publishBatchV2Frame.byteLength,
        run: () => {
            parseCgpWireData(publishBatchV2Frame, { includeRawFrame: false });
        }
    }
];

const eventIterations = 200_000;
const batchIterations = 40_000;
const results = cases.map((caseInfo) =>
    bench(
        caseInfo,
        caseInfo.name.includes("publish-batch")
            ? 5_000
            : caseInfo.name.includes("batch32")
                ? batchIterations
                : eventIterations
    )
);

console.log(JSON.stringify({
    eventFrameBytes: {
        generic: genericEventFrame.byteLength,
        compact: compactEventFrame.byteLength
    },
    batchFrameBytes: {
        generic: genericBatchFrame.byteLength,
        compact: compactBatchFrame.byteLength
    },
    publishBatchFrameBytes: {
        rawJson: publishBatchRawFrame.byteLength,
        direct: publishBatchDirectFrame.byteLength,
        binaryV2: publishBatchV2Frame.byteLength
    },
    results
}, null, 2));
