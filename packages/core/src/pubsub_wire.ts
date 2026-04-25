import {
    CgpWireFormat,
    CgpWireParseOptions,
    InvalidCgpFrameError,
    ParsedCgpFrame,
    encodeCgpFrame,
    parseCgpWireData,
    stringifyCgpFrame
} from "./wire";

export type CgpPubSubFrameKind = "PUB" | "SUB" | "UNSUB" | "ACK";

interface CompactPubSubEnvelopeLike {
    originId?: string;
    guildId?: string;
    event?: unknown;
    events?: unknown[];
    head?: unknown;
    frame?: string;
    liveTopics?: boolean;
}

const PUBSUB_MAGIC_0 = 0x43; // C
const PUBSUB_MAGIC_1 = 0x47; // G
const PUBSUB_MAGIC_2 = 0x50; // P
const PUBSUB_MAGIC_3 = 0x53; // S
const PUBSUB_VERSION = 0x01;
const PUBSUB_FLAG_EVENT = 1 << 0;
const PUBSUB_FLAG_EVENTS = 1 << 1;
const PUBSUB_FLAG_HEAD = 1 << 2;
const PUBSUB_FLAG_FRAME = 1 << 3;
const PUBSUB_FLAG_ID = 1 << 4;
const PUBSUB_FLAG_TOKEN = 1 << 5;
const PUBSUB_FLAG_LIVE_TOPICS_FALSE = 1 << 6;

const sharedTextEncoder = typeof TextEncoder !== "undefined"
    ? new TextEncoder()
    : undefined;
const sharedTextDecoder = typeof TextDecoder !== "undefined"
    ? new TextDecoder()
    : undefined;

function allocByteArray(size: number) {
    if (typeof Buffer !== "undefined") {
        return Buffer.allocUnsafe(size);
    }
    return new Uint8Array(size);
}

function utf8ToBytes(value: string) {
    if (typeof Buffer !== "undefined") {
        return Buffer.from(value, "utf8");
    }
    if (sharedTextEncoder) {
        return sharedTextEncoder.encode(value);
    }
    return Uint8Array.from(value.split("").map((char) => char.charCodeAt(0)));
}

function bytesToUtf8(bytes: Uint8Array) {
    if (typeof Buffer !== "undefined") {
        return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString("utf8");
    }
    if (sharedTextDecoder) {
        return sharedTextDecoder.decode(bytes);
    }
    return String.fromCharCode(...bytes);
}

function utf8ByteLength(value: string) {
    if (typeof Buffer !== "undefined") {
        return Buffer.byteLength(value, "utf8");
    }
    return utf8ToBytes(value).byteLength;
}

function writeUtf8(target: Uint8Array, offset: number, value: string) {
    if (typeof Buffer !== "undefined" && Buffer.isBuffer(target)) {
        return target.write(value, offset, "utf8");
    }
    const bytes = utf8ToBytes(value);
    target.set(bytes, offset);
    return bytes.byteLength;
}

function writeBytes(target: Uint8Array, offset: number, bytes: Uint8Array) {
    target.set(bytes, offset);
    return bytes.byteLength;
}

function toByteChunk(value: unknown): Uint8Array | undefined {
    if (value instanceof ArrayBuffer) {
        return new Uint8Array(value);
    }
    if (ArrayBuffer.isView(value)) {
        return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    }
    if (typeof Buffer !== "undefined" && Buffer.isBuffer(value)) {
        return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    }
    return undefined;
}

function flattenByteChunks(chunks: Uint8Array[]) {
    if (chunks.length === 1) {
        return chunks[0];
    }
    const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
    const merged = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
        merged.set(chunk, offset);
        offset += chunk.byteLength;
    }
    return merged;
}

function pubSubKindCode(kind: CgpPubSubFrameKind) {
    switch (kind) {
        case "PUB":
            return 1;
        case "SUB":
            return 2;
        case "UNSUB":
            return 3;
        case "ACK":
            return 4;
        default:
            throw new InvalidCgpFrameError(`Unsupported pubsub frame kind: ${String(kind)}`);
    }
}

function pubSubKindFromCode(code: number): CgpPubSubFrameKind {
    switch (code) {
        case 1:
            return "PUB";
        case 2:
            return "SUB";
        case 3:
            return "UNSUB";
        case 4:
            return "ACK";
        default:
            throw new InvalidCgpFrameError(`Unsupported compact pubsub frame opcode: ${code}`);
    }
}

function isCompactPubSubFrame(bytes: Uint8Array) {
    return bytes.byteLength >= 6 &&
        bytes[0] === PUBSUB_MAGIC_0 &&
        bytes[1] === PUBSUB_MAGIC_1 &&
        bytes[2] === PUBSUB_MAGIC_2 &&
        bytes[3] === PUBSUB_MAGIC_3 &&
        bytes[4] === PUBSUB_VERSION;
}

function writeUint16(target: Uint8Array, offset: number, value: number) {
    target[offset] = (value >>> 8) & 0xff;
    target[offset + 1] = value & 0xff;
}

function writeUint32(target: Uint8Array, offset: number, value: number) {
    target[offset] = (value >>> 24) & 0xff;
    target[offset + 1] = (value >>> 16) & 0xff;
    target[offset + 2] = (value >>> 8) & 0xff;
    target[offset + 3] = value & 0xff;
}

function readUint16(bytes: Uint8Array, offset: number) {
    if (offset + 2 > bytes.byteLength) {
        throw new InvalidCgpFrameError("Compact pubsub frame is truncated");
    }
    return (bytes[offset] << 8) | bytes[offset + 1];
}

function readUint32(bytes: Uint8Array, offset: number) {
    if (offset + 4 > bytes.byteLength) {
        throw new InvalidCgpFrameError("Compact pubsub frame is truncated");
    }
    return ((bytes[offset] << 24) >>> 0) |
        (bytes[offset + 1] << 16) |
        (bytes[offset + 2] << 8) |
        bytes[offset + 3];
}

function guildIdFromTopic(topic: string) {
    if (!topic.startsWith("guild:")) {
        return "";
    }
    const rest = topic.slice("guild:".length);
    const separator = rest.indexOf(":");
    return separator >= 0 ? rest.slice(0, separator) : rest;
}

function readCompactPubSubChunk(bytes: Uint8Array, offset: number) {
    const length = readUint32(bytes, offset);
    const start = offset + 4;
    const end = start + length;
    if (end > bytes.byteLength) {
        throw new InvalidCgpFrameError("Compact pubsub payload chunk is truncated");
    }
    return {
        nextOffset: end,
        bytes: bytes.subarray(start, end)
    };
}

function parseCompactPubSubJsonChunk(bytes: Uint8Array) {
    try {
        return bytes.byteLength > 0 ? JSON.parse(bytesToUtf8(bytes)) : null;
    } catch {
        throw new InvalidCgpFrameError("Compact pubsub payload chunk must be valid JSON");
    }
}

function encodeCompactPubFrame(payload: unknown) {
    const record = payload && typeof payload === "object" && !Array.isArray(payload)
        ? payload as Record<string, unknown>
        : {};
    const topic = typeof record.topic === "string" ? record.topic : "";
    const id = typeof record.id === "string" ? record.id : "";
    const token = typeof record.token === "string" ? record.token : "";
    const envelope = record.envelope && typeof record.envelope === "object" && !Array.isArray(record.envelope)
        ? record.envelope as CompactPubSubEnvelopeLike
        : {};
    const originId = typeof envelope.originId === "string" ? envelope.originId : "";
    if (!topic || !originId) {
        throw new InvalidCgpFrameError("Compact pubsub PUB frame requires topic and envelope.originId");
    }

    const topicBytes = utf8ToBytes(topic);
    const originBytes = utf8ToBytes(originId);
    const idBytes = id ? utf8ToBytes(id) : undefined;
    const tokenBytes = token ? utf8ToBytes(token) : undefined;
    const topicLength = topicBytes.byteLength;
    const originLength = originBytes.byteLength;
    const idLength = idBytes?.byteLength ?? 0;
    const tokenLength = tokenBytes?.byteLength ?? 0;
    if (topicLength > 0xffff || originLength > 0xffff || idLength > 0xffff || tokenLength > 0xffff) {
        throw new InvalidCgpFrameError("Compact pubsub PUB topic/origin/id/token is too long");
    }

    let flags = 0;
    let eventBytes: Uint8Array | undefined;
    let eventsBytes: Uint8Array | undefined;
    let headBytes: Uint8Array | undefined;
    let frameBytes: Uint8Array | undefined;
    let eventLength = 0;
    let eventsLength = 0;
    let headLength = 0;
    let frameLength = 0;
    if (id) {
        flags |= PUBSUB_FLAG_ID;
    }
    if (token) {
        flags |= PUBSUB_FLAG_TOKEN;
    }
    if (envelope.event !== undefined) {
        flags |= PUBSUB_FLAG_EVENT;
        eventBytes = utf8ToBytes(JSON.stringify(envelope.event ?? null));
        eventLength = eventBytes.byteLength;
    } else if (Array.isArray(envelope.events)) {
        flags |= PUBSUB_FLAG_EVENTS;
        eventsBytes = utf8ToBytes(JSON.stringify(envelope.events));
        eventsLength = eventsBytes.byteLength;
    }
    if (envelope.head !== undefined) {
        flags |= PUBSUB_FLAG_HEAD;
        headBytes = utf8ToBytes(JSON.stringify(envelope.head ?? null));
        headLength = headBytes.byteLength;
    }
    if (typeof envelope.frame === "string" && envelope.frame.length > 0) {
        flags |= PUBSUB_FLAG_FRAME;
        frameBytes = utf8ToBytes(envelope.frame);
        frameLength = frameBytes.byteLength;
    }
    if (envelope.liveTopics === false) {
        flags |= PUBSUB_FLAG_LIVE_TOPICS_FALSE;
    }

    const chunkBytes =
        (idBytes ? 4 + idLength : 0) +
        (tokenBytes ? 4 + tokenLength : 0) +
        (eventBytes ? 4 + eventLength : 0) +
        (eventsBytes ? 4 + eventsLength : 0) +
        (headBytes ? 4 + headLength : 0) +
        (frameBytes ? 4 + frameLength : 0);
    const frame = allocByteArray(6 + 2 + 2 + 1 + topicLength + originLength + chunkBytes);
    frame[0] = PUBSUB_MAGIC_0;
    frame[1] = PUBSUB_MAGIC_1;
    frame[2] = PUBSUB_MAGIC_2;
    frame[3] = PUBSUB_MAGIC_3;
    frame[4] = PUBSUB_VERSION;
    frame[5] = pubSubKindCode("PUB");
    writeUint16(frame, 6, topicLength);
    writeUint16(frame, 8, originLength);
    frame[10] = flags;
    let offset = 11;
    offset += writeBytes(frame, offset, topicBytes);
    offset += writeBytes(frame, offset, originBytes);
    if (idBytes) {
        writeUint32(frame, offset, idLength);
        offset += 4;
        offset += writeBytes(frame, offset, idBytes);
    }
    if (tokenBytes) {
        writeUint32(frame, offset, tokenLength);
        offset += 4;
        offset += writeBytes(frame, offset, tokenBytes);
    }
    if (eventBytes) {
        writeUint32(frame, offset, eventLength);
        offset += 4;
        offset += writeBytes(frame, offset, eventBytes);
    } else if (eventsBytes) {
        writeUint32(frame, offset, eventsLength);
        offset += 4;
        offset += writeBytes(frame, offset, eventsBytes);
    }
    if (headBytes) {
        writeUint32(frame, offset, headLength);
        offset += 4;
        offset += writeBytes(frame, offset, headBytes);
    }
    if (frameBytes) {
        writeUint32(frame, offset, frameLength);
        offset += 4;
        offset += writeBytes(frame, offset, frameBytes);
    }
    return frame;
}

function parseCompactPubFrame(bytes: Uint8Array, includeRawFrame = false): ParsedCgpFrame {
    const topicLength = readUint16(bytes, 6);
    const originLength = readUint16(bytes, 8);
    const flags = bytes[10] ?? 0;
    let offset = 11;
    const topicEnd = offset + topicLength;
    const originEnd = topicEnd + originLength;
    if (originEnd > bytes.byteLength) {
        throw new InvalidCgpFrameError("Compact pubsub PUB frame is truncated");
    }

    const topic = bytesToUtf8(bytes.subarray(offset, topicEnd));
    offset = topicEnd;
    const originId = bytesToUtf8(bytes.subarray(offset, originEnd));
    offset = originEnd;
    const envelope: CompactPubSubEnvelopeLike = {
        originId,
        guildId: guildIdFromTopic(topic)
    };
    let id: string | undefined;
    let token: string | undefined;

    if ((flags & PUBSUB_FLAG_ID) !== 0) {
        const chunk = readCompactPubSubChunk(bytes, offset);
        id = bytesToUtf8(chunk.bytes);
        offset = chunk.nextOffset;
    }
    if ((flags & PUBSUB_FLAG_TOKEN) !== 0) {
        const chunk = readCompactPubSubChunk(bytes, offset);
        token = bytesToUtf8(chunk.bytes);
        offset = chunk.nextOffset;
    }

    if ((flags & PUBSUB_FLAG_EVENT) !== 0) {
        const chunk = readCompactPubSubChunk(bytes, offset);
        envelope.event = parseCompactPubSubJsonChunk(chunk.bytes);
        offset = chunk.nextOffset;
    } else if ((flags & PUBSUB_FLAG_EVENTS) !== 0) {
        const chunk = readCompactPubSubChunk(bytes, offset);
        const parsed = parseCompactPubSubJsonChunk(chunk.bytes);
        envelope.events = Array.isArray(parsed) ? parsed : [];
        offset = chunk.nextOffset;
    }
    if ((flags & PUBSUB_FLAG_HEAD) !== 0) {
        const chunk = readCompactPubSubChunk(bytes, offset);
        envelope.head = parseCompactPubSubJsonChunk(chunk.bytes);
        offset = chunk.nextOffset;
    }
    if ((flags & PUBSUB_FLAG_FRAME) !== 0) {
        const chunk = readCompactPubSubChunk(bytes, offset);
        envelope.frame = bytesToUtf8(chunk.bytes);
        offset = chunk.nextOffset;
    }
    if ((flags & PUBSUB_FLAG_LIVE_TOPICS_FALSE) !== 0) {
        envelope.liveTopics = false;
    }
    if (offset !== bytes.byteLength) {
        throw new InvalidCgpFrameError("Compact pubsub PUB frame has trailing bytes");
    }

    const payload = {
        ...(id ? { id } : {}),
        topic,
        ...(token ? { token } : {}),
        envelope
    };
    const frame: ParsedCgpFrame = { kind: "PUB", payload };
    if (includeRawFrame) {
        frame.rawFrame = stringifyCgpFrame("PUB", payload);
    }
    return frame;
}

export function encodeCgpPubSubFrame(
    kind: CgpPubSubFrameKind,
    payload: unknown,
    wireFormat: CgpWireFormat
): string | Uint8Array {
    if (wireFormat !== "binary-v1" && wireFormat !== "binary-v2") {
        return encodeCgpFrame(kind, payload, wireFormat);
    }
    if (kind === "PUB") {
        return encodeCompactPubFrame(payload);
    }

    const payloadJson = JSON.stringify(payload ?? null);
    const payloadLength = utf8ByteLength(payloadJson);
    const frame = allocByteArray(6 + payloadLength);
    frame[0] = PUBSUB_MAGIC_0;
    frame[1] = PUBSUB_MAGIC_1;
    frame[2] = PUBSUB_MAGIC_2;
    frame[3] = PUBSUB_MAGIC_3;
    frame[4] = PUBSUB_VERSION;
    frame[5] = pubSubKindCode(kind);
    writeUtf8(frame, 6, payloadJson);
    return frame;
}

function parseCompactPubSubFrame(bytes: Uint8Array, includeRawFrame = false): ParsedCgpFrame {
    if (!isCompactPubSubFrame(bytes)) {
        throw new InvalidCgpFrameError("Frame is not a compact pubsub frame");
    }

    const kind = pubSubKindFromCode(bytes[5] ?? 0);
    if (kind === "PUB") {
        return parseCompactPubFrame(bytes, includeRawFrame);
    }
    const payloadBytes = bytes.subarray(6);
    let payload: unknown = null;
    try {
        payload = payloadBytes.byteLength > 0 ? JSON.parse(bytesToUtf8(payloadBytes)) : null;
    } catch {
        throw new InvalidCgpFrameError("Compact pubsub payload must be valid JSON");
    }

    const frame: ParsedCgpFrame = { kind, payload };
    if (includeRawFrame) {
        frame.rawFrame = stringifyCgpFrame(kind, payload);
    }
    return frame;
}

export function parseCgpPubSubData(data: unknown, options: CgpWireParseOptions = {}): ParsedCgpFrame {
    const includeRawFrame = options.includeRawFrame === true;
    const single = toByteChunk(data);
    if (single) {
        if (isCompactPubSubFrame(single)) {
            return parseCompactPubSubFrame(single, includeRawFrame);
        }
        return parseCgpWireData(single, options);
    }
    if (Array.isArray(data)) {
        const chunks: Uint8Array[] = [];
        for (const entry of data) {
            const chunk = toByteChunk(entry);
            if (chunk) chunks.push(chunk);
        }
        if (chunks.length > 0) {
            const merged = flattenByteChunks(chunks);
            if (isCompactPubSubFrame(merged)) {
                return parseCompactPubSubFrame(merged, includeRawFrame);
            }
            return parseCgpWireData(merged, options);
        }
    }
    return parseCgpWireData(data, options);
}
