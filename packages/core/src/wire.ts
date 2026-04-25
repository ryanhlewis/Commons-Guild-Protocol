export type CgpWireFormat = "json" | "binary-json" | "binary-v1" | "binary-v2";

export interface ParsedCgpFrame {
    kind: string;
    payload: unknown;
    /**
     * Compatibility JSON-array representation of the frame. This is useful when a
     * node receives a compact binary frame but needs to gossip it to peers that may
     * still negotiate JSON.
     */
    rawFrame?: string;
}

export interface ParsedCgpFrameWithRaw extends ParsedCgpFrame {
    rawFrame: string;
}

export interface CgpWireParseOptions {
    includeRawFrame?: boolean;
}

export class InvalidCgpFrameError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "InvalidCgpFrameError";
    }
}

const sharedTextEncoder = typeof TextEncoder !== "undefined"
    ? new TextEncoder()
    : undefined;
const sharedTextDecoder = typeof TextDecoder !== "undefined"
    ? new TextDecoder()
    : undefined;
const BINARY_V1_MAGIC_0 = 0x43; // C
const BINARY_V1_MAGIC_1 = 0x47; // G
const BINARY_V1_MAGIC_2 = 0x50; // P
const BINARY_V1_VERSION = 0x01;
const BINARY_V2_VERSION = 0x02;
const BINARY_V2_OPCODE_GENERIC = 0;
const BINARY_V2_OPCODE_PUBLISH = 1;
const BINARY_V2_OPCODE_PUBLISH_BATCH = 2;
const BINARY_V2_PUBLISH_FLAG_CREATED_AT = 1 << 0;
const BINARY_V2_PUBLISH_FLAG_CLIENT_EVENT_ID = 1 << 1;

function bytesToUtf8(bytes: Uint8Array) {
    if (typeof Buffer !== "undefined") {
        return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString("utf8");
    }
    if (sharedTextDecoder) {
        return sharedTextDecoder.decode(bytes);
    }
    return String.fromCharCode(...bytes);
}

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

export function encodeCgpWireFrame(frame: string, wireFormat: CgpWireFormat): string | Uint8Array {
    if (wireFormat === "binary-v1" || wireFormat === "binary-v2") {
        const { kind, payload } = parseCgpFrame(frame);
        return wireFormat === "binary-v2"
            ? encodeBinaryV2Frame(kind, payload)
            : encodeBinaryV1Frame(kind, payload);
    }
    return wireFormat === "binary-json" ? utf8ToBytes(frame) : frame;
}

export function stringifyCgpFrame(kind: string, payload: unknown) {
    return JSON.stringify([kind, payload]);
}

export function parseCgpFrame(raw: string): { kind: string; payload: unknown } {
    let parsed: unknown;
    try {
        parsed = JSON.parse(raw);
    } catch {
        throw new InvalidCgpFrameError("Frame must be valid JSON");
    }

    if (!Array.isArray(parsed) || typeof parsed[0] !== "string" || parsed[0].trim().length === 0) {
        throw new InvalidCgpFrameError("Frame must be a JSON array whose first item is a frame kind");
    }
    return { kind: parsed[0], payload: parsed[1] };
}

function normalizeFrameKind(kind: string) {
    const normalized = kind.trim();
    if (!normalized) {
        throw new InvalidCgpFrameError("Frame kind cannot be empty");
    }
    return normalized;
}

function isBinaryV1Frame(bytes: Uint8Array) {
    return bytes.byteLength >= 5 &&
        bytes[0] === BINARY_V1_MAGIC_0 &&
        bytes[1] === BINARY_V1_MAGIC_1 &&
        bytes[2] === BINARY_V1_MAGIC_2 &&
        bytes[3] === BINARY_V1_VERSION;
}

function isBinaryV2Frame(bytes: Uint8Array) {
    return bytes.byteLength >= 6 &&
        bytes[0] === BINARY_V1_MAGIC_0 &&
        bytes[1] === BINARY_V1_MAGIC_1 &&
        bytes[2] === BINARY_V1_MAGIC_2 &&
        bytes[3] === BINARY_V2_VERSION;
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
        throw new InvalidCgpFrameError("Binary-v2 frame is truncated");
    }
    return (bytes[offset] << 8) | bytes[offset + 1];
}

function readUint32(bytes: Uint8Array, offset: number) {
    if (offset + 4 > bytes.byteLength) {
        throw new InvalidCgpFrameError("Binary-v2 frame is truncated");
    }
    return ((bytes[offset] << 24) >>> 0) |
        (bytes[offset + 1] << 16) |
        (bytes[offset + 2] << 8) |
        bytes[offset + 3];
}

function writeFloat64(target: Uint8Array, offset: number, value: number) {
    new DataView(target.buffer, target.byteOffset, target.byteLength).setFloat64(offset, value, false);
}

function readFloat64(bytes: Uint8Array, offset: number) {
    if (offset + 8 > bytes.byteLength) {
        throw new InvalidCgpFrameError("Binary-v2 frame is truncated");
    }
    return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getFloat64(offset, false);
}

function encodeBinaryV1Frame(kind: string, payload: unknown): Uint8Array {
    const kindBytes = utf8ToBytes(normalizeFrameKind(kind));
    if (kindBytes.byteLength > 255) {
        throw new InvalidCgpFrameError("Frame kind is too long for binary-v1");
    }
    const payloadBytes = utf8ToBytes(JSON.stringify(payload ?? null));
    const frame = allocByteArray(5 + kindBytes.byteLength + payloadBytes.byteLength);
    frame[0] = BINARY_V1_MAGIC_0;
    frame[1] = BINARY_V1_MAGIC_1;
    frame[2] = BINARY_V1_MAGIC_2;
    frame[3] = BINARY_V1_VERSION;
    frame[4] = kindBytes.byteLength;
    frame.set(kindBytes, 5);
    frame.set(payloadBytes, 5 + kindBytes.byteLength);
    return frame;
}

function recordPayload(payload: unknown): Record<string, unknown> | undefined {
    return payload && typeof payload === "object" && !Array.isArray(payload)
        ? payload as Record<string, unknown>
        : undefined;
}

interface CompactPublishParts {
    flags: number;
    createdAt?: number;
    authorBytes: Uint8Array;
    signatureBytes: Uint8Array;
    clientEventIdBytes?: Uint8Array;
    bodyBytes: Uint8Array;
    byteLength: number;
}

function compactPublishParts(record: Record<string, unknown>): CompactPublishParts | undefined {
    if (record.body === undefined || typeof record.author !== "string" || typeof record.signature !== "string") {
        return undefined;
    }
    const bodyBytes = utf8ToBytes(JSON.stringify(record.body ?? null));
    const authorBytes = utf8ToBytes(record.author);
    const signatureBytes = utf8ToBytes(record.signature);
    const clientEventId = typeof record.clientEventId === "string" ? record.clientEventId : undefined;
    const clientEventIdBytes = clientEventId !== undefined ? utf8ToBytes(clientEventId) : undefined;
    const createdAt = Number(record.createdAt);
    const hasCreatedAt = Number.isFinite(createdAt);
    if (authorBytes.byteLength > 0xffff || signatureBytes.byteLength > 0xffff || (clientEventIdBytes?.byteLength ?? 0) > 0xffff) {
        return undefined;
    }
    let flags = 0;
    if (hasCreatedAt) flags |= BINARY_V2_PUBLISH_FLAG_CREATED_AT;
    if (clientEventIdBytes) flags |= BINARY_V2_PUBLISH_FLAG_CLIENT_EVENT_ID;
    const byteLength = 1 + 2 + 2 + 2 + 4 + (hasCreatedAt ? 8 : 0) +
        authorBytes.byteLength +
        signatureBytes.byteLength +
        (clientEventIdBytes?.byteLength ?? 0) +
        bodyBytes.byteLength;
    return {
        flags,
        ...(hasCreatedAt ? { createdAt } : {}),
        authorBytes,
        signatureBytes,
        ...(clientEventIdBytes ? { clientEventIdBytes } : {}),
        bodyBytes,
        byteLength
    };
}

function writeCompactPublishPayload(target: Uint8Array, start: number, parts: CompactPublishParts) {
    let offset = start;
    target[offset++] = parts.flags;
    writeUint16(target, offset, parts.authorBytes.byteLength);
    offset += 2;
    writeUint16(target, offset, parts.signatureBytes.byteLength);
    offset += 2;
    writeUint16(target, offset, parts.clientEventIdBytes?.byteLength ?? 0);
    offset += 2;
    writeUint32(target, offset, parts.bodyBytes.byteLength);
    offset += 4;
    if (parts.createdAt !== undefined) {
        writeFloat64(target, offset, parts.createdAt);
        offset += 8;
    }
    target.set(parts.authorBytes, offset);
    offset += parts.authorBytes.byteLength;
    target.set(parts.signatureBytes, offset);
    offset += parts.signatureBytes.byteLength;
    if (parts.clientEventIdBytes) {
        target.set(parts.clientEventIdBytes, offset);
        offset += parts.clientEventIdBytes.byteLength;
    }
    target.set(parts.bodyBytes, offset);
    return offset + parts.bodyBytes.byteLength;
}

function encodeCompactPublishPayload(record: Record<string, unknown>): Uint8Array | undefined {
    const parts = compactPublishParts(record);
    if (!parts) return undefined;
    const frame = allocByteArray(parts.byteLength);
    writeCompactPublishPayload(frame, 0, parts);
    return frame;
}

function parseJsonBytes(bytes: Uint8Array, message: string) {
    try {
        return bytes.byteLength > 0 ? JSON.parse(bytesToUtf8(bytes)) : null;
    } catch {
        throw new InvalidCgpFrameError(message);
    }
}

function parseCompactPublishPayload(bytes: Uint8Array, offset = 0) {
    if (offset + 11 > bytes.byteLength) {
        throw new InvalidCgpFrameError("Binary-v2 publish payload is truncated");
    }
    const flags = bytes[offset++];
    const authorLength = readUint16(bytes, offset);
    offset += 2;
    const signatureLength = readUint16(bytes, offset);
    offset += 2;
    const clientEventIdLength = readUint16(bytes, offset);
    offset += 2;
    const bodyLength = readUint32(bytes, offset);
    offset += 4;
    let createdAt: number | undefined;
    if ((flags & BINARY_V2_PUBLISH_FLAG_CREATED_AT) !== 0) {
        createdAt = readFloat64(bytes, offset);
        offset += 8;
    }
    const authorEnd = offset + authorLength;
    const signatureEnd = authorEnd + signatureLength;
    const clientEventIdEnd = signatureEnd + clientEventIdLength;
    const bodyEnd = clientEventIdEnd + bodyLength;
    if (bodyEnd > bytes.byteLength) {
        throw new InvalidCgpFrameError("Binary-v2 publish payload is truncated");
    }
    const author = bytesToUtf8(bytes.subarray(offset, authorEnd));
    const signature = bytesToUtf8(bytes.subarray(authorEnd, signatureEnd));
    const clientEventId = (flags & BINARY_V2_PUBLISH_FLAG_CLIENT_EVENT_ID) !== 0
        ? bytesToUtf8(bytes.subarray(signatureEnd, clientEventIdEnd))
        : undefined;
    const body = parseJsonBytes(bytes.subarray(clientEventIdEnd, bodyEnd), "Binary-v2 publish body must be valid JSON");
    return {
        nextOffset: bodyEnd,
        payload: {
            body,
            author,
            signature,
            ...(createdAt !== undefined ? { createdAt } : {}),
            ...(clientEventId !== undefined ? { clientEventId } : {})
        }
    };
}

function encodeBinaryV2GenericFrame(kind: string, payload: unknown) {
    const kindBytes = utf8ToBytes(normalizeFrameKind(kind));
    if (kindBytes.byteLength > 255) {
        throw new InvalidCgpFrameError("Frame kind is too long for binary-v2");
    }
    const payloadBytes = utf8ToBytes(JSON.stringify(payload ?? null));
    const frame = allocByteArray(6 + kindBytes.byteLength + payloadBytes.byteLength);
    frame[0] = BINARY_V1_MAGIC_0;
    frame[1] = BINARY_V1_MAGIC_1;
    frame[2] = BINARY_V1_MAGIC_2;
    frame[3] = BINARY_V2_VERSION;
    frame[4] = BINARY_V2_OPCODE_GENERIC;
    frame[5] = kindBytes.byteLength;
    frame.set(kindBytes, 6);
    frame.set(payloadBytes, 6 + kindBytes.byteLength);
    return frame;
}

function encodeBinaryV2Frame(kind: string, payload: unknown): Uint8Array {
    const normalizedKind = normalizeFrameKind(kind);
    const record = recordPayload(payload);
    if (normalizedKind === "PUBLISH" && record) {
        const compact = encodeCompactPublishPayload(record);
        if (compact) {
            const frame = allocByteArray(6 + compact.byteLength);
            frame[0] = BINARY_V1_MAGIC_0;
            frame[1] = BINARY_V1_MAGIC_1;
            frame[2] = BINARY_V1_MAGIC_2;
            frame[3] = BINARY_V2_VERSION;
            frame[4] = BINARY_V2_OPCODE_PUBLISH;
            frame[5] = 0;
            frame.set(compact, 6);
            return frame;
        }
    }
    if (normalizedKind === "PUBLISH_BATCH" && record && Array.isArray(record.events)) {
        const events: CompactPublishParts[] = [];
        for (const event of record.events) {
            const eventRecord = recordPayload(event);
            const parts = eventRecord ? compactPublishParts(eventRecord) : undefined;
            if (!parts) {
                return encodeBinaryV2GenericFrame(normalizedKind, payload);
            }
            events.push(parts);
        }
        const batchId = typeof record.batchId === "string" ? record.batchId : "";
        const batchIdBytes = utf8ToBytes(batchId);
        if (batchIdBytes.byteLength > 0xffff) {
            return encodeBinaryV2GenericFrame(normalizedKind, payload);
        }
        const eventsBytes = events.reduce((sum, event) => sum + 4 + event.byteLength, 0);
        const frame = allocByteArray(12 + batchIdBytes.byteLength + eventsBytes);
        frame[0] = BINARY_V1_MAGIC_0;
        frame[1] = BINARY_V1_MAGIC_1;
        frame[2] = BINARY_V1_MAGIC_2;
        frame[3] = BINARY_V2_VERSION;
        frame[4] = BINARY_V2_OPCODE_PUBLISH_BATCH;
        frame[5] = 0;
        writeUint16(frame, 6, batchIdBytes.byteLength);
        writeUint32(frame, 8, events.length);
        let offset = 12;
        frame.set(batchIdBytes, offset);
        offset += batchIdBytes.byteLength;
        for (const event of events) {
            writeUint32(frame, offset, event.byteLength);
            offset += 4;
            offset = writeCompactPublishPayload(frame, offset, event);
        }
        return frame;
    }
    return encodeBinaryV2GenericFrame(normalizedKind, payload);
}

function parseBinaryV1Frame(bytes: Uint8Array, includeRawFrame = false): ParsedCgpFrame {
    if (!isBinaryV1Frame(bytes)) {
        throw new InvalidCgpFrameError("Frame is not binary-v1");
    }
    const kindLength = bytes[4] ?? 0;
    const kindStart = 5;
    const kindEnd = kindStart + kindLength;
    if (kindLength <= 0 || kindEnd > bytes.byteLength) {
        throw new InvalidCgpFrameError("Invalid binary-v1 frame kind");
    }

    const kind = normalizeFrameKind(bytesToUtf8(bytes.subarray(kindStart, kindEnd)));
    const payloadBytes = bytes.subarray(kindEnd);
    let payload: unknown = null;
    try {
        payload = payloadBytes.byteLength > 0 ? JSON.parse(bytesToUtf8(payloadBytes)) : null;
    } catch {
        throw new InvalidCgpFrameError("Binary-v1 payload must be valid JSON");
    }
    const frame: ParsedCgpFrame = { kind, payload };
    if (includeRawFrame) {
        frame.rawFrame = stringifyCgpFrame(kind, payload);
    }
    return frame;
}

function parseBinaryV2Frame(bytes: Uint8Array, includeRawFrame = false): ParsedCgpFrame {
    if (!isBinaryV2Frame(bytes)) {
        throw new InvalidCgpFrameError("Frame is not binary-v2");
    }
    const opcode = bytes[4] ?? 0;
    if (opcode === BINARY_V2_OPCODE_PUBLISH) {
        const parsed = parseCompactPublishPayload(bytes, 6);
        if (parsed.nextOffset !== bytes.byteLength) {
            throw new InvalidCgpFrameError("Binary-v2 publish frame has trailing bytes");
        }
        const frame: ParsedCgpFrame = { kind: "PUBLISH", payload: parsed.payload };
        if (includeRawFrame) {
            frame.rawFrame = stringifyCgpFrame("PUBLISH", parsed.payload);
        }
        return frame;
    }
    if (opcode === BINARY_V2_OPCODE_PUBLISH_BATCH) {
        if (bytes.byteLength < 12) {
            throw new InvalidCgpFrameError("Binary-v2 publish batch frame is truncated");
        }
        const batchIdLength = readUint16(bytes, 6);
        const eventCount = readUint32(bytes, 8);
        let offset = 12;
        const batchIdEnd = offset + batchIdLength;
        if (batchIdEnd > bytes.byteLength) {
            throw new InvalidCgpFrameError("Binary-v2 publish batch frame is truncated");
        }
        const batchId = bytesToUtf8(bytes.subarray(offset, batchIdEnd));
        offset = batchIdEnd;
        const events: unknown[] = [];
        for (let index = 0; index < eventCount; index += 1) {
            const eventLength = readUint32(bytes, offset);
            offset += 4;
            const eventEnd = offset + eventLength;
            if (eventEnd > bytes.byteLength) {
                throw new InvalidCgpFrameError("Binary-v2 publish batch event is truncated");
            }
            const parsed = parseCompactPublishPayload(bytes.subarray(offset, eventEnd));
            if (parsed.nextOffset !== eventLength) {
                throw new InvalidCgpFrameError("Binary-v2 publish batch event has trailing bytes");
            }
            events.push(parsed.payload);
            offset = eventEnd;
        }
        if (offset !== bytes.byteLength) {
            throw new InvalidCgpFrameError("Binary-v2 publish batch frame has trailing bytes");
        }
        const payload = {
            ...(batchId ? { batchId } : {}),
            events
        };
        const frame: ParsedCgpFrame = { kind: "PUBLISH_BATCH", payload };
        if (includeRawFrame) {
            frame.rawFrame = stringifyCgpFrame("PUBLISH_BATCH", payload);
        }
        return frame;
    }
    if (opcode !== BINARY_V2_OPCODE_GENERIC) {
        throw new InvalidCgpFrameError(`Unsupported binary-v2 opcode: ${opcode}`);
    }
    const kindLength = bytes[5] ?? 0;
    const kindStart = 6;
    const kindEnd = kindStart + kindLength;
    if (kindLength <= 0 || kindEnd > bytes.byteLength) {
        throw new InvalidCgpFrameError("Invalid binary-v2 frame kind");
    }
    const kind = normalizeFrameKind(bytesToUtf8(bytes.subarray(kindStart, kindEnd)));
    const payload = parseJsonBytes(bytes.subarray(kindEnd), "Binary-v2 payload must be valid JSON");
    const frame: ParsedCgpFrame = { kind, payload };
    if (includeRawFrame) {
        frame.rawFrame = stringifyCgpFrame(kind, payload);
    }
    return frame;
}

export function encodeCgpFrame(kind: string, payload: unknown, wireFormat: CgpWireFormat): string | Uint8Array {
    if (wireFormat === "binary-v1" || wireFormat === "binary-v2") {
        return wireFormat === "binary-v2"
            ? encodeBinaryV2Frame(kind, payload)
            : encodeBinaryV1Frame(kind, payload);
    }
    return encodeCgpWireFrame(stringifyCgpFrame(kind, payload), wireFormat);
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

export function rawDataToUtf8String(data: unknown) {
    if (typeof data === "string") {
        return data;
    }
    const single = toByteChunk(data);
    if (single) {
        return bytesToUtf8(single);
    }
    if (Array.isArray(data)) {
        const chunks: Uint8Array[] = [];
        for (const entry of data) {
            const chunk = toByteChunk(entry);
            if (chunk) chunks.push(chunk);
        }
        if (chunks.length > 0) {
            return bytesToUtf8(flattenByteChunks(chunks));
        }
    }
    return String(data ?? "");
}

export function parseCgpWireData(data: unknown, options: CgpWireParseOptions & { includeRawFrame: true }): ParsedCgpFrameWithRaw;
export function parseCgpWireData(data: unknown, options?: CgpWireParseOptions): ParsedCgpFrame;
export function parseCgpWireData(data: unknown, options: CgpWireParseOptions = {}): ParsedCgpFrame {
    const includeRawFrame = options.includeRawFrame === true;
    if (typeof data === "string") {
        const parsed = parseCgpFrame(data);
        return includeRawFrame ? { ...parsed, rawFrame: data } : parsed;
    }
    const single = toByteChunk(data);
    if (single) {
        if (isBinaryV1Frame(single)) {
            return parseBinaryV1Frame(single, includeRawFrame);
        }
        if (isBinaryV2Frame(single)) {
            return parseBinaryV2Frame(single, includeRawFrame);
        }
        const rawFrame = bytesToUtf8(single);
        const parsed = parseCgpFrame(rawFrame);
        return includeRawFrame ? { ...parsed, rawFrame } : parsed;
    }
    if (Array.isArray(data)) {
        const chunks: Uint8Array[] = [];
        for (const entry of data) {
            const chunk = toByteChunk(entry);
            if (chunk) chunks.push(chunk);
        }
        if (chunks.length > 0) {
            const bytes = flattenByteChunks(chunks);
            if (isBinaryV1Frame(bytes)) {
                return parseBinaryV1Frame(bytes, includeRawFrame);
            }
            if (isBinaryV2Frame(bytes)) {
                return parseBinaryV2Frame(bytes, includeRawFrame);
            }
            const rawFrame = bytesToUtf8(bytes);
            const parsed = parseCgpFrame(rawFrame);
            return includeRawFrame ? { ...parsed, rawFrame } : parsed;
        }
    }
    const rawFrame = String(data ?? "");
    const parsed = parseCgpFrame(rawFrame);
    return includeRawFrame ? { ...parsed, rawFrame } : parsed;
}

export async function socketDataToUtf8String(data: unknown) {
    if (typeof data === "string") {
        return data;
    }
    const single = toByteChunk(data);
    if (single) {
        return bytesToUtf8(single);
    }
    const blobCtor = (globalThis as any).Blob;
    if (blobCtor && data instanceof blobCtor && typeof (data as any).text === "function") {
        return await (data as any).text();
    }
    return String(data ?? "");
}

export function socketDataToCgpFrame(data: unknown, options: CgpWireParseOptions & { includeRawFrame: false }): Promise<ParsedCgpFrame>;
export function socketDataToCgpFrame(data: unknown, options?: CgpWireParseOptions): Promise<ParsedCgpFrameWithRaw>;
export async function socketDataToCgpFrame(data: unknown, options: CgpWireParseOptions = { includeRawFrame: true }): Promise<ParsedCgpFrame> {
    if (typeof data === "string") {
        return parseCgpWireData(data, options);
    }
    const single = toByteChunk(data);
    if (single) {
        return parseCgpWireData(single, options);
    }
    const blobCtor = (globalThis as any).Blob;
    if (blobCtor && data instanceof blobCtor && typeof (data as any).arrayBuffer === "function") {
        return parseCgpWireData(await (data as any).arrayBuffer(), options);
    }
    return parseCgpWireData(data, options);
}
