import { createHash } from "node:crypto";
import type { StreamKind, TopologyName } from "./types.ts";

const MAGIC = 0x484c4142;
const HEADER_BYTES = 32;

export type PacketType = "upload" | "federated" | "delivery";

export interface TopologyPacket {
    type: PacketType;
    topology: TopologyName;
    streamKind: StreamKind;
    epoch: number;
    sourceId: number;
    sequence: number;
    recipientId?: number;
    originRelayIndex: number;
    sentAt: number;
    payload: Buffer;
    payloadChecksum?: number;
}

const packetTypes: Record<PacketType, number> = { upload: 1, federated: 2, delivery: 3 };
const packetTypesByCode = Object.fromEntries(Object.entries(packetTypes).map(([name, code]) => [code, name])) as Record<number, PacketType>;
const topologyCodes: Record<TopologyName, number> = {
    "turn-mesh": 1,
    "single-sfu": 2,
    "federated-sfu": 3,
    "host-star-game": 4,
    "spatial-sharded-game": 5,
    "cascaded-sfu": 6,
    "resilient-sfu": 7
};
const topologiesByCode = Object.fromEntries(Object.entries(topologyCodes).map(([name, code]) => [code, name])) as Record<number, TopologyName>;
const streamCodes: Record<StreamKind, number> = { audio: 1, video: 2, game: 3 };
const streamsByCode = Object.fromEntries(Object.entries(streamCodes).map(([name, code]) => [code, name])) as Record<number, StreamKind>;

export function encodeTopologyPacket(packet: TopologyPacket) {
    if (packet.payload.byteLength > 65535) throw new Error("Topology packet payload exceeds uint16 length");
    const buffer = Buffer.alloc(HEADER_BYTES + packet.payload.byteLength);
    buffer.writeUInt32BE(MAGIC, 0);
    buffer.writeUInt8(1, 4);
    buffer.writeUInt8(packetTypes[packet.type], 5);
    buffer.writeUInt8(topologyCodes[packet.topology], 6);
    buffer.writeUInt8(streamCodes[packet.streamKind], 7);
    buffer.writeUInt16BE(packet.epoch & 0xffff, 8);
    buffer.writeUInt16BE(packet.sourceId & 0xffff, 10);
    buffer.writeUInt16BE(packet.sequence & 0xffff, 12);
    buffer.writeUInt16BE(packet.recipientId === undefined ? 0xffff : packet.recipientId & 0xffff, 14);
    buffer.writeUInt16BE(packet.originRelayIndex & 0xffff, 16);
    buffer.writeUInt16BE(0, 18);
    buffer.writeBigUInt64BE(BigInt(Math.max(0, Math.floor(packet.sentAt))), 20);
    buffer.writeUInt16BE(packet.payload.byteLength, 28);
    buffer.writeUInt16BE(packet.payloadChecksum ?? checksum16(packet.payload), 30);
    packet.payload.copy(buffer, HEADER_BYTES);
    return buffer;
}

export function decodeTopologyPacket(buffer: Buffer): TopologyPacket {
    if (buffer.byteLength < HEADER_BYTES || buffer.readUInt32BE(0) !== MAGIC || buffer.readUInt8(4) !== 1) {
        throw new Error("Invalid topology packet header");
    }
    const payloadLength = buffer.readUInt16BE(28);
    if (buffer.byteLength !== HEADER_BYTES + payloadLength) throw new Error("Invalid topology packet length");
    const payload = buffer.subarray(HEADER_BYTES);
    const payloadChecksum = buffer.readUInt16BE(30);
    if (checksum16(payload) !== payloadChecksum) throw new Error("Topology packet checksum mismatch");
    const type = packetTypesByCode[buffer.readUInt8(5)];
    const topology = topologiesByCode[buffer.readUInt8(6)];
    const streamKind = streamsByCode[buffer.readUInt8(7)];
    if (!type || !topology || !streamKind) throw new Error("Unknown topology packet code");
    const recipientId = buffer.readUInt16BE(14);
    return {
        type,
        topology,
        streamKind,
        epoch: buffer.readUInt16BE(8),
        sourceId: buffer.readUInt16BE(10),
        sequence: buffer.readUInt16BE(12),
        recipientId: recipientId === 0xffff ? undefined : recipientId,
        originRelayIndex: buffer.readUInt16BE(16),
        sentAt: Number(buffer.readBigUInt64BE(20)),
        payload,
        payloadChecksum
    };
}

function checksum16(buffer: Buffer) {
    let checksum = 0;
    for (const byte of buffer) checksum = (checksum + byte) & 0xffff;
    return checksum;
}

function deterministicBytes(seed: string, size: number) {
    const output = Buffer.alloc(size);
    let offset = 0;
    let counter = 0;
    while (offset < size) {
        const chunk = createHash("sha256").update(`${seed}:${counter}`).digest();
        const copied = Math.min(chunk.byteLength, size - offset);
        chunk.copy(output, offset, 0, copied);
        offset += copied;
        counter += 1;
    }
    return output;
}

export function deterministicRtpPayload(runId: string, streamKind: StreamKind, sourceId: number, sequence: number, size: number) {
    const payloadSize = Math.max(12, size);
    const payload = deterministicBytes(`${runId}:${streamKind}:${sourceId}:${sequence}`, payloadSize);
    payload[0] = 0x80;
    payload[1] = streamKind === "audio" ? 111 : streamKind === "video" ? 96 : 112;
    payload.writeUInt16BE(sequence & 0xffff, 2);
    payload.writeUInt32BE(sequence * (streamKind === "audio" ? 960 : 3000), 4);
    payload.writeUInt32BE(((sourceId + 1) * 2654435761) >>> 0, 8);
    return payload;
}

export function verifyDeterministicRtpPayload(
    runId: string,
    streamKind: StreamKind,
    sourceId: number,
    sequence: number,
    payload: Buffer
) {
    const expected = deterministicRtpPayload(runId, streamKind, sourceId, sequence, payload.byteLength);
    return payload.equals(expected) &&
        payload[0] === 0x80 &&
        payload.readUInt16BE(2) === (sequence & 0xffff) &&
        payload.readUInt32BE(8) === (((sourceId + 1) * 2654435761) >>> 0);
}

export function deliveryKey(packet: Pick<TopologyPacket, "topology" | "streamKind" | "sourceId" | "sequence" | "recipientId">) {
    return `${packet.topology}:${packet.streamKind}:${packet.sourceId}:${packet.sequence}:${packet.recipientId}`;
}
