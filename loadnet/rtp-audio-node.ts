import dgram from "node:dgram";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const DATA_DIR = process.env.LOADNET_DATA_DIR || "/data";
const READY_DIR = path.join(DATA_DIR, "ready");
const METRICS_DIR = path.join(DATA_DIR, "metrics");
const NETEM_DIR = path.join(DATA_DIR, "netem");
const RUN_ID = process.env.LOADNET_RUN_ID || "rtp-audio";

function envNumber(name: string, fallback: number) {
    const value = Number(process.env[name]);
    return Number.isFinite(value) ? value : fallback;
}

function ensureDirs() {
    fs.mkdirSync(READY_DIR, { recursive: true });
    fs.mkdirSync(METRICS_DIR, { recursive: true });
    fs.mkdirSync(NETEM_DIR, { recursive: true });
}

function writeJson(filePath: string, value: unknown) {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, JSON.stringify(value, null, 2));
}

async function sleep(ms: number) {
    await new Promise((resolve) => setTimeout(resolve, ms));
}

function sha256(buffer: Buffer) {
    return createHash("sha256").update(buffer).digest("hex");
}

function roleName() {
    return process.argv[2] || process.env.LOADNET_ROLE || "help";
}

async function applyNetem() {
    const role = roleName();
    const workerId = process.env.LOADNET_WORKER_ID || "0";
    const latencyMs = envNumber("LOADNET_LATENCY_MS", 0);
    const jitterMs = envNumber("LOADNET_JITTER_MS", 0);
    const lossPercent = envNumber("LOADNET_LOSS_PERCENT", 0);
    const statusPath = path.join(NETEM_DIR, `${role}-${workerId}.json`);

    if (latencyMs <= 0 && jitterMs <= 0 && lossPercent <= 0) {
        writeJson(statusPath, { role, workerId, required: false, applied: false });
        return;
    }

    const args = ["qdisc", "replace", "dev", "eth0", "root", "netem"];
    if (latencyMs > 0 || jitterMs > 0) {
        args.push("delay", `${latencyMs}ms`);
        if (jitterMs > 0) args.push(`${jitterMs}ms`);
    }
    if (lossPercent > 0) {
        args.push("loss", `${lossPercent}%`);
    }

    const retries = Math.max(1, envNumber("LOADNET_NETEM_RETRIES", 20));
    let warning = "";
    for (let attempt = 1; attempt <= retries; attempt += 1) {
        try {
            execFileSync("tc", args, { stdio: "ignore" });
            writeJson(statusPath, { role, workerId, required: true, applied: true, args, attempt, retries });
            return;
        } catch (error: any) {
            warning = error?.message || String(error);
            await sleep(100);
        }
    }

    writeJson(statusPath, { role, workerId, required: true, applied: false, args, retries, warning });
}

interface AudioConfig {
    sampleRate: number;
    frameMs: number;
    durationMs: number;
    frequencyHz: number;
    payloadType: number;
    ssrc: number;
    sequenceBase: number;
    timestampBase: number;
    port: number;
}

function audioConfig(): AudioConfig {
    return {
        sampleRate: Math.max(8000, envNumber("LOADNET_RTP_AUDIO_SAMPLE_RATE", 48000)),
        frameMs: Math.max(5, envNumber("LOADNET_RTP_AUDIO_FRAME_MS", 20)),
        durationMs: Math.max(100, envNumber("LOADNET_RTP_AUDIO_DURATION_MS", 800)),
        frequencyHz: Math.max(20, envNumber("LOADNET_RTP_AUDIO_FREQUENCY_HZ", 997)),
        payloadType: Math.max(0, Math.min(127, envNumber("LOADNET_RTP_AUDIO_PAYLOAD_TYPE", 96))),
        ssrc: envNumber("LOADNET_RTP_AUDIO_SSRC", 0x50636d31) >>> 0,
        sequenceBase: envNumber("LOADNET_RTP_AUDIO_SEQUENCE_BASE", 12000) & 0xffff,
        timestampBase: envNumber("LOADNET_RTP_AUDIO_TIMESTAMP_BASE", 96000) >>> 0,
        port: Math.max(1, Math.min(65535, envNumber("LOADNET_RTP_AUDIO_PORT", 5004)))
    };
}

function samplesPerFrame(config: AudioConfig) {
    return Math.floor(config.sampleRate * config.frameMs / 1000);
}

function frameCount(config: AudioConfig) {
    return Math.ceil(config.durationMs / config.frameMs);
}

function pcmSample(config: AudioConfig, sampleIndex: number) {
    const radians = 2 * Math.PI * config.frequencyHz * sampleIndex / config.sampleRate;
    return Math.max(-32768, Math.min(32767, Math.round(Math.sin(radians) * 0.65 * 32767)));
}

function pcmFrame(config: AudioConfig, frameIndex: number) {
    const count = samplesPerFrame(config);
    const buffer = Buffer.alloc(count * 2);
    for (let index = 0; index < count; index += 1) {
        buffer.writeInt16LE(pcmSample(config, frameIndex * count + index), index * 2);
    }
    return buffer;
}

function expectedPcm(config: AudioConfig) {
    return Buffer.concat(Array.from({ length: frameCount(config) }, (_, frame) => pcmFrame(config, frame)));
}

function rtpPacket(config: AudioConfig, frameIndex: number) {
    const payload = pcmFrame(config, frameIndex);
    const packet = Buffer.alloc(12 + payload.byteLength);
    packet[0] = 0x80;
    packet[1] = config.payloadType & 0x7f;
    packet.writeUInt16BE((config.sequenceBase + frameIndex) & 0xffff, 2);
    packet.writeUInt32BE((config.timestampBase + frameIndex * samplesPerFrame(config)) >>> 0, 4);
    packet.writeUInt32BE(config.ssrc >>> 0, 8);
    payload.copy(packet, 12);
    return packet;
}

function parseRtpPacket(packet: Buffer, config: AudioConfig) {
    if (packet.byteLength < 12 || packet[0] >> 6 !== 2) {
        throw new Error("Invalid RTP packet");
    }
    const cc = packet[0] & 0x0f;
    const headerBytes = 12 + cc * 4;
    if (packet.byteLength < headerBytes) {
        throw new Error("Invalid RTP CSRC header");
    }
    const payloadType = packet[1] & 0x7f;
    if (payloadType !== config.payloadType) {
        throw new Error(`Unexpected RTP payload type ${payloadType}`);
    }
    const ssrc = packet.readUInt32BE(8);
    if (ssrc !== config.ssrc) {
        throw new Error(`Unexpected RTP SSRC ${ssrc}`);
    }
    const sequence = packet.readUInt16BE(2);
    const timestamp = packet.readUInt32BE(4);
    return {
        sequence,
        timestamp,
        payload: packet.subarray(headerBytes)
    };
}

function frameIndexForSequence(sequence: number, config: AudioConfig) {
    return (sequence - config.sequenceBase + 0x10000) & 0xffff;
}

function estimateFrequency(buffer: Buffer, sampleRate: number) {
    if (buffer.byteLength < 4) return 0;
    let previous = buffer.readInt16LE(0);
    let positiveCrossings = 0;
    for (let offset = 2; offset < buffer.byteLength; offset += 2) {
        const current = buffer.readInt16LE(offset);
        if (previous <= 0 && current > 0) {
            positiveCrossings += 1;
        }
        previous = current;
    }
    const seconds = (buffer.byteLength / 2) / sampleRate;
    return seconds > 0 ? positiveCrossings / seconds : 0;
}

function rms(buffer: Buffer) {
    if (buffer.byteLength < 2) return 0;
    let sum = 0;
    const count = buffer.byteLength / 2;
    for (let offset = 0; offset < buffer.byteLength; offset += 2) {
        const normalized = buffer.readInt16LE(offset) / 32768;
        sum += normalized * normalized;
    }
    return Math.sqrt(sum / count);
}

async function runSender() {
    ensureDirs();
    await applyNetem();
    const config = audioConfig();
    const targetHost = process.env.LOADNET_RTP_AUDIO_TARGET_HOST || "receiver";
    const readyPath = path.join(READY_DIR, "receiver.ready");
    const deadline = Date.now() + envNumber("LOADNET_RTP_AUDIO_READY_TIMEOUT_MS", 15000);
    while (!fs.existsSync(readyPath)) {
        if (Date.now() > deadline) {
            throw new Error("Timed out waiting for RTP receiver readiness");
        }
        await sleep(50);
    }

    const socket = dgram.createSocket("udp4");
    let sentFrames = 0;
    const startedAt = Date.now();
    try {
        for (let frame = 0; frame < frameCount(config); frame += 1) {
            const packet = rtpPacket(config, frame);
            await new Promise<void>((resolve, reject) => {
                socket.send(packet, config.port, targetHost, (error) => error ? reject(error) : resolve());
            });
            sentFrames += 1;
            await sleep(config.frameMs);
        }
    } finally {
        socket.close();
    }

    writeJson(path.join(METRICS_DIR, "sender.json"), {
        runId: RUN_ID,
        role: "rtp-audio-sender",
        sentFrames,
        bytes: sentFrames * (12 + samplesPerFrame(config) * 2),
        ms: Date.now() - startedAt,
        targetHost,
        port: config.port
    });
}

async function runReceiver() {
    ensureDirs();
    await applyNetem();
    const config = audioConfig();
    const expectedFrames = frameCount(config);
    const expectedPayloadBytes = samplesPerFrame(config) * 2;
    const socket = dgram.createSocket("udp4");
    const payloads = new Map<number, Buffer>();
    const timestampErrors: Array<{ frameIndex: number; timestamp: number; expectedTimestamp: number }> = [];
    let duplicatePackets = 0;
    let invalidPackets = 0;
    let firstPacketAt = 0;
    let lastPacketAt = 0;

    await new Promise<void>((resolve, reject) => {
        socket.once("error", reject);
        socket.bind(config.port, () => {
            socket.off("error", reject);
            resolve();
        });
    });
    writeJson(path.join(READY_DIR, "receiver.ready"), {
        runId: RUN_ID,
        port: config.port,
        sampleRate: config.sampleRate,
        frameMs: config.frameMs
    });

    const done = new Promise<void>((resolve) => {
        const timeout = setTimeout(resolve, envNumber("LOADNET_RTP_AUDIO_RECEIVE_TIMEOUT_MS", config.durationMs + 10000));
        socket.on("message", (raw) => {
            try {
                const parsed = parseRtpPacket(raw, config);
                const frameIndex = frameIndexForSequence(parsed.sequence, config);
                if (frameIndex >= expectedFrames || parsed.payload.byteLength !== expectedPayloadBytes) {
                    invalidPackets += 1;
                    return;
                }
                const expectedTimestamp = (config.timestampBase + frameIndex * samplesPerFrame(config)) >>> 0;
                if (parsed.timestamp !== expectedTimestamp) {
                    timestampErrors.push({ frameIndex, timestamp: parsed.timestamp, expectedTimestamp });
                }
                if (payloads.has(frameIndex)) {
                    duplicatePackets += 1;
                    return;
                }
                const now = Date.now();
                firstPacketAt ||= now;
                lastPacketAt = now;
                payloads.set(frameIndex, Buffer.from(parsed.payload));
                if (payloads.size >= expectedFrames) {
                    clearTimeout(timeout);
                    resolve();
                }
            } catch {
                invalidPackets += 1;
            }
        });
    });

    await done;
    socket.close();

    const missingFrames: number[] = [];
    const orderedPayloads: Buffer[] = [];
    for (let frame = 0; frame < expectedFrames; frame += 1) {
        const payload = payloads.get(frame);
        if (payload) {
            orderedPayloads.push(payload);
        } else {
            missingFrames.push(frame);
        }
    }

    const received = Buffer.concat(orderedPayloads);
    const expected = expectedPcm(config);
    const receivedHash = sha256(received);
    const expectedHash = sha256(expected);
    const receivedRatio = expectedFrames > 0 ? payloads.size / expectedFrames : 1;
    const estimatedFrequencyHz = estimateFrequency(received, config.sampleRate);
    const audioRms = rms(received);
    const frequencyErrorHz = Math.abs(estimatedFrequencyHz - config.frequencyHz);
    const ok = missingFrames.length === 0 &&
        invalidPackets === 0 &&
        timestampErrors.length === 0 &&
        receivedHash === expectedHash &&
        audioRms > 0.1 &&
        frequencyErrorHz <= envNumber("LOADNET_RTP_AUDIO_MAX_FREQUENCY_ERROR_HZ", 5);

    const summary = {
        ok,
        runId: RUN_ID,
        role: "rtp-audio-receiver",
        protocol: "rtp-pcm16le",
        sampleRate: config.sampleRate,
        frameMs: config.frameMs,
        durationMs: config.durationMs,
        frequencyHz: config.frequencyHz,
        expectedFrames,
        receivedFrames: payloads.size,
        missingFrames,
        duplicatePackets,
        invalidPackets,
        timestampErrors,
        receivedRatio,
        expectedSha256: expectedHash,
        receivedSha256: receivedHash,
        bytesReceived: received.byteLength,
        rms: Number(audioRms.toFixed(6)),
        estimatedFrequencyHz: Number(estimatedFrequencyHz.toFixed(3)),
        frequencyErrorHz: Number(frequencyErrorHz.toFixed(3)),
        firstPacketAt,
        lastPacketAt
    };

    writeJson(path.join(METRICS_DIR, "receiver.json"), summary);
    writeJson(path.join(DATA_DIR, "summary.json"), summary);
    if (!ok) {
        process.exitCode = 1;
    }
}

async function main() {
    const role = roleName();
    if (role === "sender") return runSender();
    if (role === "receiver") return runReceiver();
    console.log("usage: tsx loadnet/rtp-audio-node.ts <sender|receiver>");
}

main().catch((error) => {
    console.error(error);
    process.exit(1);
});
