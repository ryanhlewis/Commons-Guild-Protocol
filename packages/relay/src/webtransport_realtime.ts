import { EventEmitter } from "events";
import { createHash, randomBytes, X509Certificate } from "crypto";
import { readFileSync } from "fs";
import {
  CGP_REALTIME_DATAGRAM_MAX_BYTES,
  RealtimeDatagramReassembler,
  fragmentRealtimeDatagram,
} from "@cgp/core";

const importEsmFromCommonJs = new Function(
  "specifier",
  "return import(specifier)",
) as (specifier: string) => Promise<any>;

interface WebTransportDatagramWriter {
  ready: Promise<unknown>;
  write(chunk: Uint8Array): Promise<void>;
  close(): Promise<void>;
  releaseLock(): void;
}

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

interface Http3ServerLike {
  ready: Promise<unknown>;
  closed: Promise<unknown>;
  startServer(): void;
  stopServer(): void;
  sessionStream(path: string): ReadableStream<WebTransportSessionLike>;
}

export interface CgpWebTransportOptions {
  port: number;
  host?: string;
  path?: string;
  publicUrl?: string;
  certificate: string;
  privateKey: string;
  secret?: string;
  maxDatagramBytes?: number;
  maxBufferedBytes?: number;
  advertiseCertificateHash?: boolean;
}

export interface CgpWebTransportAdvertisement {
  kind: "webtransport-datagram";
  url: string;
  certificateHash?: string;
  maxDatagramBytes: number;
}

function positiveInteger(value: string | undefined, fallback: number) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0
    ? Math.floor(parsed)
    : fallback;
}

function normalizePath(value: string | undefined) {
  const path = value?.trim() || "/cgp/realtime";
  return path.startsWith("/") ? path : `/${path}`;
}

function certificateSha256(certificate: string) {
  try {
    return createHash("sha256")
      .update(new X509Certificate(certificate).raw)
      .digest("hex");
  } catch {
    return undefined;
  }
}

export function webTransportOptionsFromEnv(
  options?: CgpWebTransportOptions,
) {
  if (options) {
    return options;
  }

  const port = positiveInteger(
    process.env.CGP_RELAY_WEBTRANSPORT_PORT,
    0,
  );
  const certificatePath =
    process.env.CGP_RELAY_WEBTRANSPORT_CERT_PATH?.trim();
  const privateKeyPath =
    process.env.CGP_RELAY_WEBTRANSPORT_KEY_PATH?.trim();
  if (port <= 0 || !certificatePath || !privateKeyPath) {
    return undefined;
  }

  return {
    port,
    host: process.env.CGP_RELAY_WEBTRANSPORT_HOST?.trim() || "0.0.0.0",
    path: normalizePath(process.env.CGP_RELAY_WEBTRANSPORT_PATH),
    publicUrl: process.env.CGP_RELAY_WEBTRANSPORT_PUBLIC_URL?.trim(),
    certificate: readFileSync(certificatePath, "utf8"),
    privateKey: readFileSync(privateKeyPath, "utf8"),
    secret:
      process.env.CGP_RELAY_WEBTRANSPORT_SECRET?.trim() ||
      randomBytes(32).toString("hex"),
    maxDatagramBytes: positiveInteger(
      process.env.CGP_RELAY_WEBTRANSPORT_MAX_DATAGRAM_BYTES,
      CGP_REALTIME_DATAGRAM_MAX_BYTES,
    ),
    maxBufferedBytes: positiveInteger(
      process.env.CGP_RELAY_WEBTRANSPORT_MAX_BUFFERED_BYTES,
      512 * 1024,
    ),
    advertiseCertificateHash:
      process.env.CGP_RELAY_WEBTRANSPORT_ADVERTISE_CERT_HASH === "1",
  } satisfies CgpWebTransportOptions;
}

export class CgpWebTransportSocket extends EventEmitter {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readonly protocol = "cgp-realtime-v1";
  readonly url: string;
  readyState = CgpWebTransportSocket.OPEN;
  bufferedAmount = 0;

  private readonly writer: WebTransportDatagramWriter;
  private readonly reassembler = new RealtimeDatagramReassembler();
  private readonly maxDatagramBytes: number;
  private readonly maxBufferedBytes: number;
  private nextMessageId = randomBytes(4).readUInt32BE(0);
  private closeEmitted = false;

  constructor(
    private readonly session: WebTransportSessionLike,
    options: {
      url: string;
      maxDatagramBytes: number;
      maxBufferedBytes: number;
    },
  ) {
    super();
    this.url = options.url;
    this.maxDatagramBytes = options.maxDatagramBytes;
    this.maxBufferedBytes = options.maxBufferedBytes;
    this.writer = session.datagrams
      .createWritable()
      .getWriter() as WebTransportDatagramWriter;

    void session.closed.then(
      () => this.finishClose(),
      () => this.finishClose(),
    );
    void this.readLoop();
    void this.readIncomingStreams();
  }

  send(data: string | ArrayBuffer | ArrayBufferView) {
    if (this.readyState !== CgpWebTransportSocket.OPEN) {
      throw new Error("WebTransport session is not open");
    }
    const bytes =
      typeof data === "string"
        ? new TextEncoder().encode(data)
        : data instanceof ArrayBuffer
          ? new Uint8Array(data)
          : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    if (bytes.byteLength > this.maxDatagramBytes - 13) {
      if (this.bufferedAmount + bytes.byteLength > this.maxBufferedBytes) {
        throw new Error("WebTransport stream queue is backpressured");
      }
      this.bufferedAmount += bytes.byteLength;
      void this.sendStream(bytes).finally(() => {
        this.bufferedAmount = Math.max(
          0,
          this.bufferedAmount - bytes.byteLength,
        );
      });
      return;
    }
    const fragments = fragmentRealtimeDatagram(
      bytes,
      this.nextMessageId++,
      this.maxDatagramBytes,
    );
    const queuedBytes = fragments.reduce(
      (total, fragment) => total + fragment.byteLength,
      0,
    );
    if (this.bufferedAmount + queuedBytes > this.maxBufferedBytes) {
      throw new Error("WebTransport datagram queue is backpressured");
    }

    this.bufferedAmount += queuedBytes;
    for (const fragment of fragments) {
      void this.writer.ready
        .then(() => this.writer.write(fragment))
        .catch(() => this.close(1011, "Datagram write failed"))
        .finally(() => {
          this.bufferedAmount = Math.max(
            0,
            this.bufferedAmount - fragment.byteLength,
          );
        });
    }
  }

  close(code = 1000, reason = "Session closed") {
    if (
      this.readyState === CgpWebTransportSocket.CLOSED ||
      this.readyState === CgpWebTransportSocket.CLOSING
    ) {
      return;
    }
    this.readyState = CgpWebTransportSocket.CLOSING;
    try {
      this.session.close({
        closeCode: Math.max(0, Math.min(0xffffffff, code)),
        reason: reason.slice(0, 1024),
      });
    } finally {
      this.finishClose();
    }
  }

  terminate() {
    this.close(1011, "Session terminated");
  }

  private async readLoop() {
    const reader = this.session.datagrams.readable.getReader();
    try {
      while (this.readyState === CgpWebTransportSocket.OPEN) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }
        const frame = this.reassembler.push(value);
        if (frame) {
          this.emit("message", Buffer.from(frame));
        }
      }
    } catch {
      // Session closure is surfaced through the close event.
    } finally {
      reader.releaseLock();
      this.finishClose();
    }
  }

  private async sendStream(bytes: Uint8Array) {
    try {
      const stream = await this.session.createUnidirectionalStream({
        sendOrder: 0,
      });
      const writer = stream.getWriter();
      try {
        await writer.write(bytes);
        await writer.close();
      } finally {
        writer.releaseLock();
      }
    } catch {
      this.close(1011, "WebTransport stream write failed");
    }
  }

  private async readIncomingStreams() {
    const reader = this.session.incomingUnidirectionalStreams.getReader();
    try {
      while (this.readyState === CgpWebTransportSocket.OPEN) {
        const { done, value: stream } = await reader.read();
        if (done || !stream) {
          break;
        }
        void this.readStreamFrame(stream);
      }
    } catch {
      if (this.readyState === CgpWebTransportSocket.OPEN) {
        this.close(1011, "WebTransport stream receive failed");
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
      const frame = Buffer.allocUnsafe(totalBytes);
      let offset = 0;
      for (const chunk of chunks) {
        frame.set(chunk, offset);
        offset += chunk.byteLength;
      }
      this.emit("message", frame);
    } finally {
      reader.releaseLock();
    }
  }

  private finishClose() {
    if (this.closeEmitted) {
      return;
    }
    this.closeEmitted = true;
    this.readyState = CgpWebTransportSocket.CLOSED;
    this.reassembler.clear();
    try {
      this.writer.releaseLock();
    } catch {
      // The stream may already have released the writer during shutdown.
    }
    this.emit("close");
  }
}

export class CgpWebTransportRelayServer {
  private server?: Http3ServerLike;
  private sessionReader?: ReadableStreamDefaultReader<WebTransportSessionLike>;
  private readonly sockets = new Set<CgpWebTransportSocket>();
  private ready = false;

  readonly advertisement: CgpWebTransportAdvertisement;

  constructor(
    private readonly options: CgpWebTransportOptions,
    private readonly onConnection: (socket: CgpWebTransportSocket) => void,
  ) {
    const host = options.host?.trim() || "0.0.0.0";
    const path = normalizePath(options.path);
    const publicHost = host === "0.0.0.0" ? "127.0.0.1" : host;
    this.advertisement = {
      kind: "webtransport-datagram",
      url:
        options.publicUrl?.trim() ||
        `https://${publicHost}:${options.port}${path}`,
      ...(options.advertiseCertificateHash
        ? { certificateHash: certificateSha256(options.certificate) }
        : {}),
      maxDatagramBytes:
        options.maxDatagramBytes ?? CGP_REALTIME_DATAGRAM_MAX_BYTES,
    };
  }

  async start() {
    const module = await importEsmFromCommonJs(
      "@fails-components/webtransport",
    );
    await module.quicheLoaded;
    const server = new module.Http3Server({
      port: this.options.port,
      host: this.options.host?.trim() || "0.0.0.0",
      secret: this.options.secret || randomBytes(32).toString("hex"),
      cert: this.options.certificate,
      privKey: this.options.privateKey,
      reliability: "unreliableOnly",
      defaultDatagramsReadableMode: "bytes",
    }) as Http3ServerLike;
    this.server = server;
    this.sessionReader = server
      .sessionStream(normalizePath(this.options.path))
      .getReader();
    server.startServer();
    await server.ready;
    this.ready = true;
    void this.acceptLoop();
  }

  isReady() {
    return this.ready;
  }

  async close() {
    this.ready = false;
    for (const socket of this.sockets) {
      socket.close(1001, "Relay shutting down");
    }
    this.sockets.clear();
    try {
      await this.sessionReader?.cancel();
    } catch {
      // Reader cancellation races normal server shutdown.
    }
    this.sessionReader = undefined;
    if (!this.server) {
      return;
    }
    this.server.stopServer();
    await Promise.race([
      this.server.closed.catch(() => undefined),
      new Promise((resolve) => setTimeout(resolve, 500)),
    ]);
    this.server = undefined;
  }

  private async acceptLoop() {
    const reader = this.sessionReader;
    if (!reader) {
      return;
    }
    try {
      while (this.ready) {
        const { done, value: session } = await reader.read();
        if (done || !session) {
          break;
        }
        await session.ready;
        const socket = new CgpWebTransportSocket(session, {
          url: this.advertisement.url,
          maxDatagramBytes: this.advertisement.maxDatagramBytes,
          maxBufferedBytes: this.options.maxBufferedBytes ?? 512 * 1024,
        });
        this.sockets.add(socket);
        socket.once("close", () => this.sockets.delete(socket));
        this.onConnection(socket);
      }
    } catch (error) {
      if (this.ready) {
        console.error("Relay WebTransport session loop failed:", error);
      }
    }
  }
}
