const DATAGRAM_MAGIC = Uint8Array.of(0x43, 0x47, 0x50, 0x44);
const DATAGRAM_VERSION = 1;
const DATAGRAM_HEADER_BYTES = 13;

export const CGP_REALTIME_DATAGRAM_MAX_BYTES = 1_150;
export const CGP_REALTIME_DATAGRAM_HEADER_BYTES = DATAGRAM_HEADER_BYTES;

interface PendingAssembly {
  createdAt: number;
  fragmentCount: number;
  fragments: Array<Uint8Array | undefined>;
  receivedFragments: number;
  receivedBytes: number;
}

export interface RealtimeDatagramReassemblerOptions {
  maxFrameBytes?: number;
  maxPendingAssemblies?: number;
  maxAssemblyAgeMs?: number;
}

function writeUint32(target: Uint8Array, offset: number, value: number) {
  new DataView(target.buffer, target.byteOffset, target.byteLength).setUint32(
    offset,
    value >>> 0,
    false,
  );
}

function writeUint16(target: Uint8Array, offset: number, value: number) {
  new DataView(target.buffer, target.byteOffset, target.byteLength).setUint16(
    offset,
    value,
    false,
  );
}

function readUint32(source: Uint8Array, offset: number) {
  return new DataView(
    source.buffer,
    source.byteOffset,
    source.byteLength,
  ).getUint32(offset, false);
}

function readUint16(source: Uint8Array, offset: number) {
  return new DataView(
    source.buffer,
    source.byteOffset,
    source.byteLength,
  ).getUint16(offset, false);
}

function hasDatagramHeader(packet: Uint8Array) {
  return (
    packet.byteLength >= DATAGRAM_HEADER_BYTES &&
    packet[0] === DATAGRAM_MAGIC[0] &&
    packet[1] === DATAGRAM_MAGIC[1] &&
    packet[2] === DATAGRAM_MAGIC[2] &&
    packet[3] === DATAGRAM_MAGIC[3] &&
    packet[4] === DATAGRAM_VERSION
  );
}

export function fragmentRealtimeDatagram(
  frame: Uint8Array,
  messageId: number,
  maxDatagramBytes = CGP_REALTIME_DATAGRAM_MAX_BYTES,
) {
  const payloadBytes = Math.floor(maxDatagramBytes) - DATAGRAM_HEADER_BYTES;
  if (payloadBytes <= 0) {
    throw new Error("Realtime datagram size is smaller than its header");
  }

  const fragmentCount = Math.max(1, Math.ceil(frame.byteLength / payloadBytes));
  if (fragmentCount > 0xffff) {
    throw new Error("Realtime frame requires too many datagram fragments");
  }

  const fragments: Uint8Array[] = [];
  for (let index = 0; index < fragmentCount; index += 1) {
    const start = index * payloadBytes;
    const end = Math.min(frame.byteLength, start + payloadBytes);
    const fragment = new Uint8Array(DATAGRAM_HEADER_BYTES + end - start);
    fragment.set(DATAGRAM_MAGIC, 0);
    fragment[4] = DATAGRAM_VERSION;
    writeUint32(fragment, 5, messageId);
    writeUint16(fragment, 9, index);
    writeUint16(fragment, 11, fragmentCount);
    fragment.set(frame.subarray(start, end), DATAGRAM_HEADER_BYTES);
    fragments.push(fragment);
  }
  return fragments;
}

export class RealtimeDatagramReassembler {
  private readonly pending = new Map<number, PendingAssembly>();
  private readonly maxFrameBytes: number;
  private readonly maxPendingAssemblies: number;
  private readonly maxAssemblyAgeMs: number;

  constructor(options: RealtimeDatagramReassemblerOptions = {}) {
    this.maxFrameBytes = Math.max(1, options.maxFrameBytes ?? 128 * 1024);
    this.maxPendingAssemblies = Math.max(
      1,
      options.maxPendingAssemblies ?? 128,
    );
    this.maxAssemblyAgeMs = Math.max(1, options.maxAssemblyAgeMs ?? 2_000);
  }

  push(packet: Uint8Array, now = Date.now()) {
    this.prune(now);
    if (!hasDatagramHeader(packet)) {
      return undefined;
    }

    const messageId = readUint32(packet, 5);
    const fragmentIndex = readUint16(packet, 9);
    const fragmentCount = readUint16(packet, 11);
    if (fragmentCount === 0 || fragmentIndex >= fragmentCount) {
      return undefined;
    }

    let assembly = this.pending.get(messageId);
    if (!assembly) {
      if (this.pending.size >= this.maxPendingAssemblies) {
        const oldest = this.pending.keys().next().value as number | undefined;
        if (oldest !== undefined) {
          this.pending.delete(oldest);
        }
      }
      assembly = {
        createdAt: now,
        fragmentCount,
        fragments: new Array(fragmentCount),
        receivedFragments: 0,
        receivedBytes: 0,
      };
      this.pending.set(messageId, assembly);
    } else if (assembly.fragmentCount !== fragmentCount) {
      this.pending.delete(messageId);
      return undefined;
    }

    if (assembly.fragments[fragmentIndex]) {
      return undefined;
    }

    const payload = packet.slice(DATAGRAM_HEADER_BYTES);
    if (assembly.receivedBytes + payload.byteLength > this.maxFrameBytes) {
      this.pending.delete(messageId);
      return undefined;
    }
    assembly.fragments[fragmentIndex] = payload;
    assembly.receivedFragments += 1;
    assembly.receivedBytes += payload.byteLength;

    if (assembly.receivedFragments !== assembly.fragmentCount) {
      return undefined;
    }

    const frame = new Uint8Array(assembly.receivedBytes);
    let offset = 0;
    for (const fragment of assembly.fragments) {
      if (!fragment) {
        this.pending.delete(messageId);
        return undefined;
      }
      frame.set(fragment, offset);
      offset += fragment.byteLength;
    }
    this.pending.delete(messageId);
    return frame;
  }

  prune(now = Date.now()) {
    for (const [messageId, assembly] of this.pending) {
      if (now - assembly.createdAt >= this.maxAssemblyAgeMs) {
        this.pending.delete(messageId);
      }
    }
  }

  clear() {
    this.pending.clear();
  }
}
