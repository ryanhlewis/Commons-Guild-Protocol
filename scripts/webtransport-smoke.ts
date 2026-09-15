import { execFileSync } from "child_process";
import { createHash, X509Certificate } from "crypto";
import { mkdtempSync, readFileSync, rmSync } from "fs";
import { tmpdir } from "os";
import path from "path";
import dgram from "dgram";
import { CgpClient } from "@cgp/client";
import { CgpWebTransportDatagramClient } from "@cgp/client";
import {
  EventBody,
  generatePrivateKey,
  getPublicKey,
  verifyObject,
} from "@cgp/core";
import { RelayServer } from "@cgp/relay/src/server";
import { MemoryStore } from "@cgp/relay/src/store";

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function reserveUdpPort() {
  const socket = dgram.createSocket("udp4");
  return await new Promise<number>((resolve, reject) => {
    socket.once("error", reject);
    socket.bind(0, "127.0.0.1", () => {
      const address = socket.address();
      socket.close(() => resolve(address.port));
    });
  });
}

async function waitFor<T>(
  label: string,
  predicate: () => T | undefined | false,
  timeoutMs = 5_000,
) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    const value = predicate();
    if (value) {
      return value;
    }
    await sleep(20);
  }
  throw new Error(`Timed out waiting for ${label}`);
}

function createCertificate(directory: string) {
  const certificatePath = path.join(directory, "cert.pem");
  const privateKeyPath = path.join(directory, "key.pem");
  execFileSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "ec",
      "-pkeyopt",
      "ec_paramgen_curve:prime256v1",
      "-nodes",
      "-keyout",
      privateKeyPath,
      "-out",
      certificatePath,
      "-days",
      "13",
      "-subj",
      "/CN=127.0.0.1",
      "-addext",
      "subjectAltName=IP:127.0.0.1",
      "-config",
      process.platform === "win32" ? "NUL" : "/dev/null",
    ],
    { stdio: "ignore" },
  );
  const certificate = readFileSync(certificatePath, "utf8");
  return {
    certificate,
    privateKey: readFileSync(privateKeyPath, "utf8"),
    hash: createHash("sha256")
      .update(new X509Certificate(certificate).raw)
      .digest("hex"),
  };
}

async function main() {
  const tempDirectory = mkdtempSync(path.join(tmpdir(), "cgp-webtransport-"));
  const webTransportPort = await reserveUdpPort();
  const certificate = createCertificate(tempDirectory);
  const ownerPrivateKey = generatePrivateKey();
  const ownerKeyPair = {
    priv: ownerPrivateKey,
    pub: getPublicKey(ownerPrivateKey),
  };
  const relay = new RelayServer(0, new MemoryStore(), [], {
    enableDefaultPlugins: false,
    webTransport: {
      port: webTransportPort,
      host: "127.0.0.1",
      publicUrl: `https://127.0.0.1:${webTransportPort}/cgp/realtime`,
      path: "/cgp/realtime",
      certificate: certificate.certificate,
      privateKey: certificate.privateKey,
      advertiseCertificateHash: true,
    },
  });
  const relayPort = await waitFor("relay websocket port", () => {
    const value = relay.getPort();
    return Number.isFinite(value) && value > 0 ? value : undefined;
  });
  const owner = new CgpClient({
    relays: [`ws://127.0.0.1:${relayPort}`],
    keyPair: ownerKeyPair,
  });
  const sender = new CgpWebTransportDatagramClient({
    url: `https://127.0.0.1:${webTransportPort}/cgp/realtime`,
    certificateHash: certificate.hash,
  });
  const observer = new CgpWebTransportDatagramClient({
    url: `https://127.0.0.1:${webTransportPort}/cgp/realtime`,
    certificateHash: certificate.hash,
  });

  try {
    await owner.connect();
    const guildId = await owner.createGuild("WebTransport Smoke");
    const channelId = await owner.createChannel(
      guildId,
      "voice",
      "voice",
    );
    await Promise.all([sender.connect(), observer.connect()]);
    await observer.subscribeTransient(guildId, ownerKeyPair, [channelId]);

    const events: any[] = [];
    observer.on("event", (event) => events.push(event));
    const capturedAt = Date.now();
    const body = {
      type: "CALL_EVENT",
      guildId,
      channelId,
      roomId: "webtransport-smoke",
      expiresAt: capturedAt + 1_000,
      payload: {
        kind: "fallback-audio",
        messageId: "webtransport-smoke-audio",
        capturedAt,
        audioCodec: "ima-adpcm",
        audioBase64: Buffer.alloc(164, 7).toString("base64"),
      },
    } as EventBody;
    if (!(await sender.publishTransient(body, ownerKeyPair))) {
      throw new Error("WebTransport publish was backpressured");
    }
    const event = await waitFor("signed transient event", () =>
      events.find(
        (candidate) =>
          candidate?.body?.payload?.messageId ===
          "webtransport-smoke-audio",
      ),
    );
    if (!verifyObject(event.author, {
      body: event.body,
      author: event.author,
      createdAt: event.createdAt,
    }, event.signature)) {
      throw new Error("Received WebTransport event signature is invalid");
    }
    const screenBytes = Buffer.alloc(8_192, 11);
    const screenCapturedAt = Date.now();
    const screenBody = {
      type: "CALL_EVENT",
      guildId,
      channelId,
      roomId: "webtransport-smoke",
      expiresAt: screenCapturedAt + 1_000,
      payload: {
        kind: "fallback-video",
        streamKind: "screen",
        messageId: "webtransport-smoke-screen",
        capturedAt: screenCapturedAt,
        frame: `data:image/jpeg;base64,${screenBytes.toString("base64")}`,
      },
    } as EventBody;
    if (!(await sender.publishTransient(screenBody, ownerKeyPair))) {
      throw new Error("WebTransport stream publish was backpressured");
    }
    const screenEvent = await waitFor("streamed transient event", () =>
      events.find(
        (candidate) =>
          candidate?.body?.payload?.messageId ===
          "webtransport-smoke-screen",
      ),
    );
    const receivedScreen = Buffer.from(
      screenEvent.body.payload.frame.split(",").pop(),
      "base64",
    );
    if (!receivedScreen.equals(screenBytes)) {
      throw new Error("WebTransport stream payload was corrupted");
    }
    console.log(
      JSON.stringify({
        ok: true,
        transport: "webtransport-datagram-and-stream",
        observedLatencyMs: Date.now() - capturedAt,
        payloadBytes: 164,
        streamedPayloadBytes: screenBytes.byteLength,
        signatureVerified: true,
        streamPayloadVerified: true,
      }),
    );
  } finally {
    await Promise.allSettled([sender.close(), observer.close()]);
    owner.close();
    await relay.close();
    rmSync(tempDirectory, { recursive: true, force: true });
  }
}

void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
