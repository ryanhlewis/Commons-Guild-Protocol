import { EventEmitter } from "events";
import {
  CGP_REALTIME_DATAGRAM_MAX_BYTES,
  EventBody,
  RealtimeDatagramReassembler,
  fragmentRealtimeDatagram,
  hashObject,
  parseCgpWireData,
  sign,
} from "@cgp/core";

const importEsmFromCommonJs = new Function(
  "specifier",
  "return import(specifier)",
) as (specifier: string) => Promise<any>;

const randomBytes = (byteLength: number) => {
  const bytes = globalThis.crypto.getRandomValues(new Uint8Array(byteLength));
  return Buffer.from(bytes);
};

interface WebTransportSessionLike {
  ready: Promise<void>;
  closed: Promise<unknown>;
  incomingUnidirectionalStreams: ReadableStream<ReadableStream<Uint8Array>>;
  createUnidirectionalStream(options?: {
    sendOrder?: number;
  }): Promise<WritableStream<Uint8Array>>;
  datagrams: {
    readable: ReadableStream<Uint8Array>;
    createWritable(): WritableStream<Uint8Array>;
  };
  close(info?: { closeCode: number; reason: string }): void;
}

export interface CgpRealtimeDatagramTransport {
  readonly kind: "webtransport-datagram";
  readonly bufferedAmount: number;
  readonly connected: boolean;
  connect(): Promise<void>;
  sendFrame(kind: string, payload: unknown): boolean;
  close(): Promise<void>;
}

export interface CgpWebTransportDatagramClientOptions {
  url: string;
  certificateHash?: string;
  maxDatagramBytes?: number;
  maxBufferedBytes?: number;
  connectTimeoutMs?: number;
}

export interface CgpRealtimeKeyPair {
  pub: string;
  priv: Uint8Array;
}

export interface CgpPreparedTransientPayload {
  body: EventBody;
  author: string;
  createdAt: number;
  signature: string;
  clientEventId: string;
  ackRequested: false;
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string) {
  return new Promise<T>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
      timeoutMs,
    );
    timeout.unref?.();
    void promise.then(
      (value) => {
        clearTimeout(timeout);
        resolve(value);
      },
      (error) => {
        clearTimeout(timeout);
        reject(error);
      },
    );
  });
}

export class CgpWebTransportDatagramClient
  extends EventEmitter
  implements CgpRealtimeDatagramTransport
{
  readonly kind = "webtransport-datagram" as const;
  bufferedAmount = 0;
  connected = false;

  private session?: WebTransportSessionLike;
  private writer?: WritableStreamDefaultWriter<Uint8Array>;
  private readonly reassembler = new RealtimeDatagramReassembler();
  private nextMessageId = randomBytes(4).readUInt32BE(0);
  private closing = false;

  constructor(private readonly options: CgpWebTransportDatagramClientOptions) {
    super();
  }

  async connect() {
    if (this.connected) {
      return;
    }
    const module = await importEsmFromCommonJs(
      "@fails-components/webtransport",
    );
    await module.quicheLoaded;
    const certificateHash = this.options.certificateHash?.trim();
    const session = new module.WebTransport(this.options.url, {
      requireUnreliable: true,
      congestionControl: "low-latency",
      ...(certificateHash
        ? {
            serverCertificateHashes: [
              {
                algorithm: "sha-256",
                value: Buffer.from(certificateHash, "hex"),
              },
            ],
          }
        : {}),
    }) as WebTransportSessionLike;
    void session.closed.catch(() => undefined);
    const timeoutMs = Math.max(100, this.options.connectTimeoutMs ?? 5_000);
    await withTimeout(session.ready, timeoutMs, "WebTransport connection");
    this.session = session;
    this.writer = session.datagrams.createWritable().getWriter();
    this.connected = true;
    this.closing = false;
    void session.closed.then(
      () => this.finishClose(),
      () => this.finishClose(),
    );
    void this.readLoop(session);
    void this.readIncomingStreams(session);
    const hello = this.waitForFrame("HELLO_OK", undefined, timeoutMs);
    const helloPayload = {
      protocol: "cgp/0.1",
      wireFormat: "json",
      supportedWireFormats: ["json"],
    };
    const helloRetry = setInterval(() => {
      this.sendFrame("HELLO", helloPayload);
    }, 200);
    helloRetry.unref?.();
    try {
      if (!this.sendFrame("HELLO", helloPayload)) {
        throw new Error("Failed to send WebTransport HELLO");
      }
      await hello;
    } finally {
      clearInterval(helloRetry);
    }
  }

  sendFrame(kind: string, payload: unknown) {
    if (!this.connected || !this.writer || this.closing) {
      return false;
    }
    const bytes = new TextEncoder().encode(JSON.stringify([kind, payload]));
    const maxDatagramBytes =
      this.options.maxDatagramBytes ?? CGP_REALTIME_DATAGRAM_MAX_BYTES;
    const fragments = fragmentRealtimeDatagram(
      bytes,
      this.nextMessageId++,
      maxDatagramBytes,
    );
    const queuedBytes = fragments.reduce(
      (total, fragment) => total + fragment.byteLength,
      0,
    );
    if (
      this.bufferedAmount + queuedBytes >
      (this.options.maxBufferedBytes ?? 512 * 1024)
    ) {
      return false;
    }

    this.bufferedAmount += queuedBytes;
    for (const fragment of fragments) {
      void this.writer.ready
        .then(() => this.writer?.write(fragment))
        .catch((error) => {
          this.emit("error", error);
          this.finishClose();
        })
        .finally(() => {
          this.bufferedAmount = Math.max(
            0,
            this.bufferedAmount - fragment.byteLength,
          );
        });
    }
    return true;
  }

  async subscribeTransient(
    guildId: string,
    keyPair: CgpRealtimeKeyPair,
    channels?: string[],
  ) {
    const subId = `realtime-${randomBytes(8).toString("hex")}`;
    const createdAt = Date.now();
    const unsignedPayload = {
      subId,
      guildId,
      ...(channels?.length ? { channels } : {}),
      author: keyPair.pub,
      createdAt,
    };
    const signature = await sign(
      keyPair.priv,
      hashObject({ kind: "SUB_TRANSIENT", payload: unsignedPayload }),
    );
    const subscribed = this.waitForFrame(
      "SUB_TRANSIENT_OK",
      (payload) =>
        typeof payload === "object" &&
        payload !== null &&
        (payload as { subId?: string }).subId === subId,
      5_000,
    );
    const subscriptionPayload = {
      ...unsignedPayload,
      signature,
    };
    const subscriptionRetry = setInterval(() => {
      this.sendFrame("SUB_TRANSIENT", subscriptionPayload);
    }, 200);
    subscriptionRetry.unref?.();
    try {
      if (!this.sendFrame("SUB_TRANSIENT", subscriptionPayload)) {
        return false;
      }
      await subscribed;
      return true;
    } finally {
      clearInterval(subscriptionRetry);
    }
  }

  async publishTransient(
    body: EventBody,
    keyPair: CgpRealtimeKeyPair,
    clientEventId = `${Date.now()}-${randomBytes(6).toString("hex")}`,
  ) {
    const payload = await this.prepareTransient(
      body,
      keyPair,
      clientEventId,
    );
    return await this.publishPreparedTransient(payload);
  }

  async prepareTransient(
    body: EventBody,
    keyPair: CgpRealtimeKeyPair,
    clientEventId = `${Date.now()}-${randomBytes(6).toString("hex")}`,
  ): Promise<CgpPreparedTransientPayload> {
    const createdAt = Date.now();
    const unsigned = { body, author: keyPair.pub, createdAt };
    const signature = await sign(keyPair.priv, hashObject(unsigned));
    return {
      ...unsigned,
      signature,
      clientEventId,
      ackRequested: false as const,
    };
  }

  async publishPreparedTransient(payload: CgpPreparedTransientPayload) {
    const bodyWithDeadline = payload.body as unknown as {
      expiresAt?: unknown;
    };
    const expiresAt =
      typeof bodyWithDeadline.expiresAt === "number"
        ? bodyWithDeadline.expiresAt
        : undefined;
    if (expiresAt !== undefined && expiresAt <= Date.now()) {
      return false;
    }
    const bytes = new TextEncoder().encode(
      JSON.stringify(["PUBLISH_TRANSIENT", payload]),
    );
    if (
      bytes.byteLength <=
      (this.options.maxDatagramBytes ?? CGP_REALTIME_DATAGRAM_MAX_BYTES) - 13
    ) {
      return this.sendFrame("PUBLISH_TRANSIENT", payload);
    }
    return await this.sendStreamFrame(bytes, expiresAt);
  }

  async close() {
    if (this.closing) {
      return;
    }
    this.closing = true;
    this.connected = false;
    try {
      await this.writer?.close();
    } catch {
      // Session close below is authoritative.
    }
    try {
      this.session?.close({ closeCode: 0, reason: "Client closed" });
    } catch {
      // Session may already be closed.
    }
    this.finishClose();
  }

  private async readLoop(session: WebTransportSessionLike) {
    const reader = session.datagrams.readable.getReader();
    try {
      while (this.connected && !this.closing) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }
        const frame = this.reassembler.push(value);
        if (!frame) {
          continue;
        }
        this.emitParsedFrame(frame);
      }
    } catch (error) {
      if (!this.closing) {
        this.emit("error", error);
      }
    } finally {
      reader.releaseLock();
      this.finishClose();
    }
  }

  private async sendStreamFrame(bytes: Uint8Array, expiresAt?: number) {
    if (!this.connected || !this.session || this.closing) {
      return false;
    }
    if (
      this.bufferedAmount + bytes.byteLength >
      (this.options.maxBufferedBytes ?? 512 * 1024)
    ) {
      return false;
    }
    const remainingMs =
      expiresAt === undefined ? 5_000 : Math.max(0, expiresAt - Date.now());
    if (remainingMs === 0) {
      return false;
    }

    this.bufferedAmount += bytes.byteLength;
    let writer: WritableStreamDefaultWriter<Uint8Array> | undefined;
    try {
      const stream = await withTimeout(
        this.session.createUnidirectionalStream({ sendOrder: 0 }),
        remainingMs,
        "WebTransport stream creation",
      );
      writer = stream.getWriter();
      await withTimeout(
        writer.write(bytes).then(() => writer!.close()),
        Math.max(
          1,
          expiresAt === undefined ? 5_000 : expiresAt - Date.now(),
        ),
        "WebTransport stream write",
      );
      return true;
    } catch (error) {
      try {
        await writer?.abort(error);
      } catch {
        // The stream may already have expired or reset.
      }
      return false;
    } finally {
      this.bufferedAmount = Math.max(0, this.bufferedAmount - bytes.byteLength);
      try {
        writer?.releaseLock();
      } catch {
        // The stream can release itself after a reset.
      }
    }
  }

  private async readIncomingStreams(session: WebTransportSessionLike) {
    const reader = session.incomingUnidirectionalStreams.getReader();
    try {
      while (this.connected && !this.closing) {
        const { done, value: stream } = await reader.read();
        if (done || !stream) {
          break;
        }
        void this.readStreamFrame(stream);
      }
    } catch (error) {
      if (!this.closing) {
        this.emit("error", error);
      }
    } finally {
      reader.releaseLock();
    }
  }

  private async readStreamFrame(stream: ReadableStream<Uint8Array>) {
    const reader = stream.getReader();
    const chunks: Uint8Array[] = [];
    let totalBytes = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }
        totalBytes += value.byteLength;
        if (totalBytes > 128 * 1024) {
          return;
        }
        chunks.push(value);
      }
      const frame = new Uint8Array(totalBytes);
      let offset = 0;
      for (const chunk of chunks) {
        frame.set(chunk, offset);
        offset += chunk.byteLength;
      }
      this.emitParsedFrame(frame);
    } finally {
      reader.releaseLock();
    }
  }

  private emitParsedFrame(frame: Uint8Array) {
    const { kind, payload } = parseCgpWireData(frame, {
      includeRawFrame: false,
    });
    this.emit("frame", kind, payload);
    if (kind === "EVENT") {
      this.emit("event", payload);
    }
  }

  private waitForFrame(
    kind: string,
    predicate?: (payload: unknown) => boolean,
    timeoutMs = 5_000,
  ) {
    return new Promise<unknown>((resolve, reject) => {
      const timeout = setTimeout(() => {
        cleanup();
        reject(new Error(`Timed out waiting for ${kind}`));
      }, timeoutMs);
      timeout.unref?.();
      const onFrame = (receivedKind: string, payload: unknown) => {
        if (receivedKind !== kind || (predicate && !predicate(payload))) {
          return;
        }
        cleanup();
        resolve(payload);
      };
      const onClose = () => {
        cleanup();
        reject(new Error(`WebTransport closed while waiting for ${kind}`));
      };
      const cleanup = () => {
        clearTimeout(timeout);
        this.off("frame", onFrame);
        this.off("close", onClose);
      };
      this.on("frame", onFrame);
      this.on("close", onClose);
    });
  }

  private finishClose() {
    if (!this.connected && !this.session && !this.writer) {
      return;
    }
    this.connected = false;
    this.closing = true;
    this.reassembler.clear();
    try {
      this.writer?.releaseLock();
    } catch {
      // Writer may already be released.
    }
    this.writer = undefined;
    this.session = undefined;
    this.emit("close");
  }
}
