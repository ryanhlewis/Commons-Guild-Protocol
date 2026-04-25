import fs from "node:fs";
import path from "node:path";
import { once } from "node:events";
import readline from "node:readline";
import WebSocket from "ws";
import type { RawData } from "ws";
import { CgpClient } from "@cgp/client";
import { LevelStore } from "@cgp/relay";
import {
    GuildEvent,
    RelayHead,
    assertRelayHeadQuorum,
    computeEventId,
    generatePrivateKey,
    getPublicKey,
    hashObject,
    sign,
    summarizeRelayHeadQuorum,
    verify,
    verifyRelayHead
} from "@cgp/core";

function argValue(name: string, fallback?: string) {
    const prefix = `--${name}=`;
    const found = process.argv.find((arg) => arg.startsWith(prefix));
    if (found) return found.slice(prefix.length);
    const index = process.argv.indexOf(`--${name}`);
    return index >= 0 ? process.argv[index + 1] : fallback;
}

function hasFlag(name: string) {
    return process.argv.includes(`--${name}`);
}

function positiveIntegerArg(name: string, fallback: number, max = Number.MAX_SAFE_INTEGER) {
    const parsed = Number(argValue(name, String(fallback)));
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return fallback;
    }
    return Math.min(max, Math.floor(parsed));
}

function command() {
    return process.argv.find((arg, index) => index > 1 && !arg.startsWith("--")) || "help";
}

function verifyEvent(event: GuildEvent, previous?: GuildEvent) {
    const errors: string[] = [];
    if (previous) {
        if (event.seq !== previous.seq + 1) {
            errors.push(`seq ${event.seq} does not follow ${previous.seq}`);
        }
        if (event.prevHash !== previous.id) {
            errors.push(`prevHash mismatch at seq ${event.seq}`);
        }
    } else {
        if (event.seq !== 0) {
            errors.push(`first seq is ${event.seq}, expected 0`);
        }
        if (event.prevHash !== null) {
            errors.push("genesis prevHash must be null");
        }
    }

    const expectedId = computeEventId(event);
    if (expectedId !== event.id) {
        errors.push(`event id mismatch at seq ${event.seq}`);
    }

    if (!verify(event.author, hashObject({ body: event.body, author: event.author, createdAt: event.createdAt }), event.signature)) {
        errors.push(`signature mismatch at seq ${event.seq}`);
    }

    return errors;
}

async function* iterateGuildEvents(store: LevelStore, guildId: string): AsyncIterable<GuildEvent> {
    const iterable = store.iterateLog?.(guildId);
    if (iterable) {
        for await (const event of iterable) {
            yield event;
        }
        return;
    }
    for (const event of await store.getLog(guildId)) {
        yield event;
    }
}

async function verifyGuildLog(store: LevelStore, guildId: string) {
    const errors: string[] = [];
    let previous: GuildEvent | undefined;
    let count = 0;
    for await (const event of iterateGuildEvents(store, guildId)) {
        errors.push(...verifyEvent(event, previous));
        previous = event;
        count += 1;
    }
    return {
        guildId,
        ok: errors.length === 0,
        events: count,
        headSeq: previous?.seq ?? -1,
        headHash: previous?.id ?? null,
        errors
    };
}

async function writeStream(stream: fs.WriteStream, chunk: string) {
    if (!stream.write(chunk)) {
        await once(stream, "drain");
    }
}

async function closeWriteStream(stream: fs.WriteStream) {
    stream.end();
    await once(stream, "finish");
}

function privateKeyFromHex(hex?: string) {
    if (!hex) return generatePrivateKey();
    const normalized = hex.trim().replace(/^0x/i, "");
    if (!/^[0-9a-f]+$/i.test(normalized) || normalized.length !== 64) {
        throw new Error("--private-key-hex must be a 32-byte hex string");
    }
    return Uint8Array.from(Buffer.from(normalized, "hex"));
}

async function signedReadPayload(kind: string, payload: Record<string, unknown>, privateKey: Uint8Array) {
    const author = getPublicKey(privateKey);
    const createdAt = Date.now();
    const unsignedPayload = { ...payload, author, createdAt };
    const signature = await sign(privateKey, hashObject({ kind, payload: unsignedPayload }));
    return { ...unsignedPayload, signature };
}

async function openSocket(url: string) {
    const socket = new WebSocket(url, { perMessageDeflate: false });
    await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
            socket.terminate();
            reject(new Error(`Timed out connecting to ${url}`));
        }, 5000);
        timeout.unref?.();
        socket.once("open", resolve);
        socket.once("error", reject);
        socket.once("open", () => clearTimeout(timeout));
        socket.once("error", () => clearTimeout(timeout));
    });
    return socket;
}

async function closeSocket(socket: WebSocket) {
    if (socket.readyState === WebSocket.CLOSED) {
        return;
    }
    await new Promise<void>((resolve) => {
        const timeout = setTimeout(() => {
            if (socket.readyState !== WebSocket.CLOSED) {
                socket.terminate();
            }
            socket.removeAllListeners();
            resolve();
        }, 1000);
        timeout.unref?.();
        socket.once("close", () => {
            clearTimeout(timeout);
            socket.removeAllListeners();
            resolve();
        });
        if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
            socket.close(1000, "sync complete");
        } else {
            socket.terminate();
        }
    });
}

async function requestRelayFrame<T>(
    socket: WebSocket,
    kind: string,
    payload: Record<string, unknown>,
    responseKind: string,
    subId: string,
    timeoutMs = 15000
): Promise<T> {
    return await new Promise<T>((resolve, reject) => {
        const timeout = setTimeout(() => {
            cleanup();
            reject(new Error(`Timed out waiting for ${responseKind}`));
        }, timeoutMs);
        timeout.unref?.();

        const onMessage = (raw: RawData) => {
            let frame: [string, any];
            try {
                frame = JSON.parse(raw.toString());
            } catch {
                return;
            }
            const [receivedKind, receivedPayload] = frame;
            if (receivedPayload?.subId !== subId) return;
            if (receivedKind === "ERROR") {
                cleanup();
                reject(new Error(receivedPayload.message || receivedPayload.code || `${kind} failed`));
                return;
            }
            if (receivedKind === responseKind) {
                cleanup();
                resolve(receivedPayload as T);
            }
        };
        const onError = (error: Error) => {
            cleanup();
            reject(error);
        };
        const cleanup = () => {
            clearTimeout(timeout);
            socket.off("message", onMessage);
            socket.off("error", onError);
        };

        socket.on("message", onMessage);
        socket.on("error", onError);
        socket.send(JSON.stringify([kind, payload]));
    });
}

async function requestRelayHead(
    relayUrl: string,
    guildId: string,
    privateKey: Uint8Array,
    timeoutMs = 10000
): Promise<{ relay: string; head?: RelayHead; error?: string }> {
    let socket: WebSocket | undefined;
    const subId = `head-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
    try {
        socket = await openSocket(relayUrl);
        const payload = await signedReadPayload("GET_HEAD", { subId, guildId }, privateKey);
        const response = await requestRelayFrame<{ head?: RelayHead }>(
            socket,
            "GET_HEAD",
            payload,
            "RELAY_HEAD",
            subId,
            timeoutMs
        );
        return { relay: relayUrl, head: response.head };
    } catch (error: any) {
        return { relay: relayUrl, error: error?.message || String(error) };
    } finally {
        if (socket) {
            await closeSocket(socket).catch(() => undefined);
        }
    }
}

interface RelaySyncResult {
    ok: boolean;
    guildId: string;
    relay: string;
    appended: number;
    requests: number;
    localHeadSeq: number;
    localHeadHash: string | null;
    remoteEndSeq: number;
    remoteEndHash: string | null;
}

async function syncStoreFromRelay(options: {
    store: LevelStore;
    guildId: string;
    relayUrl: string;
    privateKey: Uint8Array;
    limit: number;
    expectedEndSeq?: number;
    expectedEndHash?: string | null;
}): Promise<RelaySyncResult> {
    const socket = await openSocket(options.relayUrl);
    let appended = 0;
    let requests = 0;
    let previous = await options.store.getLastEvent(options.guildId);
    let afterSeq = previous?.seq ?? -1;
    let remoteEndSeq = -1;
    let remoteEndHash: string | null = null;

    try {
        while (true) {
            const subId = `sync-${Date.now().toString(36)}-${requests}`;
            const payload = await signedReadPayload("GET_LOG_RANGE", {
                subId,
                guildId: options.guildId,
                afterSeq,
                limit: options.limit
            }, options.privateKey);
            const response = await requestRelayFrame<{
                events?: GuildEvent[];
                hasMore?: boolean;
                nextAfterSeq?: number;
                endSeq?: number;
                endHash?: string | null;
            }>(socket, "GET_LOG_RANGE", payload, "LOG_RANGE", subId);
            requests += 1;

            const events = Array.isArray(response.events) ? response.events : [];
            remoteEndSeq = typeof response.endSeq === "number" ? response.endSeq : remoteEndSeq;
            remoteEndHash = typeof response.endHash === "string" || response.endHash === null ? response.endHash : remoteEndHash;

            if (
                options.expectedEndSeq !== undefined &&
                remoteEndSeq >= 0 &&
                remoteEndSeq !== options.expectedEndSeq
            ) {
                throw new Error(`Source relay head seq ${remoteEndSeq} does not match expected canonical seq ${options.expectedEndSeq}`);
            }
            if (
                options.expectedEndHash !== undefined &&
                remoteEndHash !== null &&
                remoteEndHash !== options.expectedEndHash
            ) {
                throw new Error(`Source relay head hash ${remoteEndHash} does not match expected canonical hash ${options.expectedEndHash}`);
            }

            if (events.length === 0) {
                if (response.hasMore && typeof response.nextAfterSeq === "number" && response.nextAfterSeq > afterSeq) {
                    throw new Error("Source relay did not expose a contiguous log range; use an authorized sync key");
                }
                break;
            }

            for (const event of events) {
                const errors = verifyEvent(event, previous);
                if (errors.length > 0) {
                    throw new Error(`Remote event verification failed at seq ${event.seq}: ${errors.join("; ")}`);
                }
                previous = event;
            }

            await options.store.appendEvents(options.guildId, events);
            appended += events.length;
            afterSeq = previous?.seq ?? afterSeq;

            if (!response.hasMore || afterSeq >= remoteEndSeq) {
                break;
            }
        }

        const verification = await verifyGuildLog(options.store, options.guildId);
        const ok = verification.ok && (remoteEndSeq < 0 || verification.headSeq === remoteEndSeq) &&
            (!remoteEndHash || verification.headHash === remoteEndHash);
        return {
            ok,
            guildId: options.guildId,
            relay: options.relayUrl,
            appended,
            requests,
            localHeadSeq: verification.headSeq,
            localHeadHash: verification.headHash,
            remoteEndSeq,
            remoteEndHash
        };
    } finally {
        await closeSocket(socket);
    }
}

async function runVerifyLog() {
    const db = argValue("db");
    if (!db) throw new Error("verify-log requires --db <path>");
    const guild = argValue("guild");
    const store = new LevelStore(path.resolve(db));
    try {
        const guildIds = guild ? [guild] : await store.getGuildIds();
        const results = [];
        for (const guildId of guildIds) {
            results.push(await verifyGuildLog(store, guildId));
        }
        const ok = results.every((result) => result.ok);
        console.log(JSON.stringify({ ok, guilds: results }, null, 2));
        if (!ok) process.exitCode = 1;
    } finally {
        await store.close();
    }
}

async function runExportGuild() {
    const db = argValue("db");
    const guildId = argValue("guild");
    const output = argValue("output");
    if (!db || !guildId || !output) throw new Error("export-guild requires --db <path> --guild <id> --output <file>");
    const store = new LevelStore(path.resolve(db));
    try {
        const events = await store.getLog(guildId);
        const verification = await verifyGuildLog(store, guildId);
        const exportBody = {
            format: "cgp.guild-log-export.v1",
            exportedAt: new Date().toISOString(),
            guildId,
            verification,
            events
        };
        fs.mkdirSync(path.dirname(path.resolve(output)), { recursive: true });
        fs.writeFileSync(path.resolve(output), JSON.stringify(exportBody, null, 2));
        console.log(JSON.stringify({ ok: verification.ok, output: path.resolve(output), events: events.length }, null, 2));
        if (!verification.ok) process.exitCode = 1;
    } finally {
        await store.close();
    }
}

async function runBackupDb() {
    const db = argValue("db");
    const output = argValue("output");
    if (!db || !output) throw new Error("backup-db requires --db <path> --output <file>");
    const store = new LevelStore(path.resolve(db));
    try {
        const guildIds = await store.getGuildIds();
        const guilds = [];
        for (const guildId of guildIds) {
            const events = await store.getLog(guildId);
            const verification = await verifyGuildLog(store, guildId);
            guilds.push({ guildId, verification, events });
        }
        const ok = guilds.every((guild) => guild.verification.ok);
        const backupBody = {
            format: "cgp.relay-backup.v1",
            exportedAt: new Date().toISOString(),
            guilds
        };
        fs.mkdirSync(path.dirname(path.resolve(output)), { recursive: true });
        fs.writeFileSync(path.resolve(output), JSON.stringify(backupBody, null, 2));
        console.log(JSON.stringify({ ok, output: path.resolve(output), guilds: guilds.length }, null, 2));
        if (!ok) process.exitCode = 1;
    } finally {
        await store.close();
    }
}

async function runBackupJsonl() {
    const db = argValue("db");
    const output = argValue("output");
    if (!db || !output) throw new Error("backup-jsonl requires --db <path> --output <file>");

    const outputPath = path.resolve(output);
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    const store = new LevelStore(path.resolve(db));
    const stream = fs.createWriteStream(outputPath, { encoding: "utf8" });
    const guildResults = [];

    try {
        await writeStream(stream, `${JSON.stringify({
            type: "backup-header",
            format: "cgp.relay-backup-jsonl.v1",
            exportedAt: new Date().toISOString()
        })}\n`);

        const guildIds = await store.getGuildIds();
        for (const guildId of guildIds) {
            await writeStream(stream, `${JSON.stringify({ type: "guild-start", guildId })}\n`);
            for await (const event of iterateGuildEvents(store, guildId)) {
                await writeStream(stream, `${JSON.stringify({ type: "event", guildId, event })}\n`);
            }
            const verification = await verifyGuildLog(store, guildId);
            guildResults.push(verification);
            await writeStream(stream, `${JSON.stringify({ type: "guild-end", guildId, verification })}\n`);
        }

        const ok = guildResults.every((result) => result.ok);
        await writeStream(stream, `${JSON.stringify({
            type: "backup-footer",
            ok,
            guilds: guildResults.length
        })}\n`);
        await closeWriteStream(stream);
        console.log(JSON.stringify({ ok, output: outputPath, guilds: guildResults.length }, null, 2));
        if (!ok) process.exitCode = 1;
    } finally {
        if (!stream.closed) {
            stream.destroy();
        }
        await store.close();
    }
}

async function runRestoreDb() {
    const db = argValue("db");
    const input = argValue("input");
    if (!db || !input) throw new Error("restore-db requires --db <path> --input <file>");
    const raw = JSON.parse(fs.readFileSync(path.resolve(input), "utf8"));
    if (raw?.format !== "cgp.relay-backup.v1" || !Array.isArray(raw.guilds)) {
        throw new Error("Unsupported relay backup format");
    }

    fs.mkdirSync(path.resolve(db), { recursive: true });
    const store = new LevelStore(path.resolve(db));
    try {
        const restored = [];
        for (const guild of raw.guilds) {
            const guildId = typeof guild.guildId === "string" ? guild.guildId : "";
            const events = Array.isArray(guild.events) ? guild.events as GuildEvent[] : [];
            if (!guildId || events.length === 0) {
                continue;
            }
            if (guild.verification && guild.verification.ok !== true) {
                throw new Error(`Backup declares guild ${guildId} failed verification`);
            }

            let previousBackupEvent: GuildEvent | undefined;
            for (const event of events) {
                const errors = verifyEvent(event, previousBackupEvent);
                if (event?.body?.guildId !== guildId) {
                    errors.push(`event guildId ${event?.body?.guildId ?? "<missing>"} does not match backup guild ${guildId}`);
                }
                if (errors.length > 0) {
                    throw new Error(`Invalid backup event for guild ${guildId}: ${errors.join("; ")}`);
                }
                previousBackupEvent = event;
            }

            const existingHead = await store.getLastEvent(guildId);
            if (existingHead) {
                const matchingBackupHead = events[existingHead.seq];
                if (!matchingBackupHead || matchingBackupHead.id !== existingHead.id) {
                    throw new Error(`Target DB already has divergent guild ${guildId} at seq ${existingHead.seq}`);
                }
            }

            const startSeq = existingHead ? existingHead.seq + 1 : 0;
            const toAppend = events.filter((event) => event.seq >= startSeq);
            if (toAppend.length > 0) {
                if (store.appendEvents) {
                    await store.appendEvents(guildId, toAppend);
                } else {
                    for (const event of toAppend) {
                        await store.append(guildId, event);
                    }
                }
            }
            restored.push({
                guildId,
                appended: toAppend.length,
                headSeq: events[events.length - 1]?.seq ?? -1,
                headHash: events[events.length - 1]?.id ?? null
            });
        }

        const verification = [];
        for (const result of restored) {
            verification.push(await verifyGuildLog(store, result.guildId));
        }
        const ok = verification.every((result) => result.ok);
        console.log(JSON.stringify({ ok, restored, verification }, null, 2));
        if (!ok) process.exitCode = 1;
    } finally {
        await store.close();
    }
}

async function runRestoreJsonl() {
    const db = argValue("db");
    const input = argValue("input");
    if (!db || !input) throw new Error("restore-jsonl requires --db <path> --input <file>");

    fs.mkdirSync(path.resolve(db), { recursive: true });
    const store = new LevelStore(path.resolve(db));
    const inputPath = path.resolve(input);
    const pending = new Map<string, GuildEvent[]>();
    const previousByGuild = new Map<string, GuildEvent | undefined>();
    const existingHeads = new Map<string, GuildEvent | undefined>();
    const restored = new Map<string, { appended: number; skipped: number }>();
    let sawHeader = false;

    const flush = async (guildId: string) => {
        const events = pending.get(guildId);
        if (!events || events.length === 0) return;
        await store.appendEvents(guildId, events);
        const current = restored.get(guildId) || { appended: 0, skipped: 0 };
        current.appended += events.length;
        restored.set(guildId, current);
        pending.set(guildId, []);
    };

    try {
        const rl = readline.createInterface({
            input: fs.createReadStream(inputPath, { encoding: "utf8" }),
            crlfDelay: Infinity
        });

        for await (const line of rl) {
            if (!line.trim()) continue;
            const record = JSON.parse(line);
            if (record.type === "backup-header") {
                if (record.format !== "cgp.relay-backup-jsonl.v1") {
                    throw new Error("Unsupported relay JSONL backup format");
                }
                sawHeader = true;
                continue;
            }
            if (!sawHeader) {
                throw new Error("Missing relay JSONL backup header");
            }
            if (record.type === "event") {
                const guildId = typeof record.guildId === "string" ? record.guildId : "";
                const event = record.event as GuildEvent;
                if (!guildId || !event) {
                    throw new Error("Malformed JSONL event record");
                }

                const previous = previousByGuild.get(guildId);
                const errors = verifyEvent(event, previous);
                if (errors.length > 0) {
                    throw new Error(`Invalid backup event for guild ${guildId}: ${errors.join("; ")}`);
                }
                previousByGuild.set(guildId, event);

                if (!existingHeads.has(guildId)) {
                    existingHeads.set(guildId, await store.getLastEvent(guildId));
                }
                const existingHead = existingHeads.get(guildId);
                const current = restored.get(guildId) || { appended: 0, skipped: 0 };
                if (existingHead && event.seq <= existingHead.seq) {
                    if (event.seq === existingHead.seq && event.id !== existingHead.id) {
                        throw new Error(`Target DB already has divergent guild ${guildId} at seq ${existingHead.seq}`);
                    }
                    current.skipped += 1;
                    restored.set(guildId, current);
                    continue;
                }

                const queue = pending.get(guildId) || [];
                queue.push(event);
                pending.set(guildId, queue);
                if (queue.length >= 512) {
                    await flush(guildId);
                    existingHeads.set(guildId, event);
                }
            } else if (record.type === "guild-end") {
                const guildId = typeof record.guildId === "string" ? record.guildId : "";
                if (guildId) {
                    await flush(guildId);
                    const verified = await verifyGuildLog(store, guildId);
                    if (!verified.ok) {
                        throw new Error(`Restored guild ${guildId} failed verification`);
                    }
                }
            }
        }

        const verification = [];
        for (const guildId of restored.keys()) {
            verification.push(await verifyGuildLog(store, guildId));
        }
        const ok = verification.every((result) => result.ok);
        console.log(JSON.stringify({
            ok,
            restored: Array.from(restored.entries()).map(([guildId, counts]) => ({ guildId, ...counts })),
            verification
        }, null, 2));
        if (!ok) process.exitCode = 1;
    } finally {
        await store.close();
    }
}

async function runSyncFromRelay() {
    const db = argValue("db");
    const guildId = argValue("guild");
    const relayUrl = argValue("relay");
    if (!db || !guildId || !relayUrl) {
        throw new Error("sync-from-relay requires --db <path> --guild <id> --relay <ws://relay>");
    }

    const limit = positiveIntegerArg("limit", 10000, 10000);
    const privateKey = privateKeyFromHex(argValue("private-key-hex"));
    const trustSourceHead = hasFlag("trust-source-head");
    const expectedEndSeqRaw = argValue("expected-end-seq");
    const expectedEndHashRaw = argValue("expected-end-hash");
    if (!trustSourceHead && (expectedEndSeqRaw === undefined || expectedEndHashRaw === undefined)) {
        throw new Error("sync-from-relay requires --expected-end-seq and --expected-end-hash, or explicit --trust-source-head");
    }
    const expectedEndSeq = expectedEndSeqRaw === undefined ? undefined : Number(expectedEndSeqRaw);
    if (expectedEndSeq !== undefined && (!Number.isSafeInteger(expectedEndSeq) || expectedEndSeq < 0)) {
        throw new Error("--expected-end-seq must be a non-negative safe integer");
    }
    const expectedEndHash = expectedEndHashRaw === undefined
        ? undefined
        : expectedEndHashRaw === "null"
            ? null
            : expectedEndHashRaw;
    const store = new LevelStore(path.resolve(db));
    try {
        const result = await syncStoreFromRelay({
            store,
            guildId,
            relayUrl,
            privateKey,
            limit,
            expectedEndSeq,
            expectedEndHash
        });
        console.log(JSON.stringify(result, null, 2));
        if (!result.ok) process.exitCode = 1;
    } finally {
        await store.close();
    }
}

async function runRepairFromQuorum() {
    const db = argValue("db");
    const guildId = argValue("guild");
    const relaySpec = argValue("relays");
    if (!db || !guildId || !relaySpec) {
        throw new Error("repair-from-quorum requires --db <leveldb-path> --guild <id> --relays <ws://a,ws://b>");
    }

    const relays = relaySpec.split(",").map((relay) => relay.trim()).filter(Boolean);
    if (relays.length === 0) {
        throw new Error("repair-from-quorum requires at least one relay");
    }

    const privateKey = privateKeyFromHex(argValue("private-key-hex"));
    const limit = positiveIntegerArg("limit", 10000, 10000);
    const minValidHeads = positiveIntegerArg("min-valid-heads", Math.max(1, Math.ceil(relays.length / 2)));
    const minCanonicalCount = positiveIntegerArg("min-canonical-count", minValidHeads);
    const requireNoConflicts = !hasFlag("allow-conflicts");
    const timeoutMs = positiveIntegerArg("timeout-ms", 10000);

    const probes = await Promise.all(relays.map((relay) => requestRelayHead(relay, guildId, privateKey, timeoutMs)));
    const relayHeads = probes.filter((probe): probe is { relay: string; head: RelayHead } => !!probe.head);
    const validRelayHeads = relayHeads.filter((probe) => verifyRelayHead(probe.head));
    const heads = relayHeads.map((probe) => probe.head);
    const quorum = assertRelayHeadQuorum(guildId, heads, { minValidHeads, minCanonicalCount, requireNoConflicts });
    const canonical = quorum.canonical;
    if (!canonical) {
        throw new Error("No canonical relay head found");
    }

    const source = validRelayHeads.find((probe) =>
        probe.head.headSeq === canonical.seq &&
        probe.head.headHash === canonical.hash
    );
    if (!source) {
        throw new Error("No source relay matched canonical head");
    }

    const store = new LevelStore(path.resolve(db));
    try {
        const localHead = await store.getLastEvent(guildId);
        if (localHead && localHead.seq > canonical.seq) {
            throw new Error(`Local relay head seq ${localHead.seq} is ahead of canonical seq ${canonical.seq}; refusing destructive rollback`);
        }
        if (localHead && localHead.seq === canonical.seq && localHead.id !== canonical.hash) {
            throw new Error(`Local relay has divergent head at canonical seq ${canonical.seq}`);
        }

        const sync = localHead?.seq === canonical.seq && localHead?.id === canonical.hash
            ? {
                ok: true,
                guildId,
                relay: source.relay,
                appended: 0,
                requests: 0,
                localHeadSeq: localHead.seq,
                localHeadHash: localHead.id,
                remoteEndSeq: canonical.seq,
                remoteEndHash: canonical.hash
            }
            : await syncStoreFromRelay({
                store,
                guildId,
                relayUrl: source.relay,
                privateKey,
                limit,
                expectedEndSeq: canonical.seq,
                expectedEndHash: canonical.hash
            });

        const ok = sync.ok && sync.localHeadSeq === canonical.seq && sync.localHeadHash === canonical.hash;
        console.log(JSON.stringify({
            ok,
            guildId,
            sourceRelay: source.relay,
            probes: probes.map((probe) => ({
                relay: probe.relay,
                headSeq: probe.head?.headSeq ?? null,
                headHash: probe.head?.headHash ?? null,
                valid: probe.head ? verifyRelayHead(probe.head) : false,
                error: probe.error
            })),
            quorum: {
                validHeads: quorum.validHeads.length,
                invalidHeads: quorum.invalidHeads.length,
                conflictCount: quorum.conflicts.length,
                canonical
            },
            sync
        }, null, 2));
        if (!ok) process.exitCode = 1;
    } finally {
        await store.close();
    }
}

async function runRepairIndexes() {
    const db = argValue("db");
    if (!db) throw new Error("repair-indexes requires --db <path>");
    const guild = argValue("guild");
    const store = new LevelStore(path.resolve(db));
    try {
        const repaired = await store.repairIndexes(guild);
        const verification = [];
        for (const result of repaired) {
            verification.push(await verifyGuildLog(store, result.guildId));
        }
        const ok = verification.every((result) => result.ok);
        console.log(JSON.stringify({ ok, repaired, verification }, null, 2));
        if (!ok) process.exitCode = 1;
    } finally {
        await store.close();
    }
}

async function runCompactDb() {
    const db = argValue("db");
    if (!db) throw new Error("compact-db requires --db <path>");
    const start = argValue("start");
    const end = argValue("end");
    const verifyAfter = hasFlag("verify");
    const store = new LevelStore(path.resolve(db));
    try {
        const startedAt = Date.now();
        await store.compact({ start, end });
        const verification = [];
        if (verifyAfter) {
            for (const guildId of await store.getGuildIds()) {
                verification.push(await verifyGuildLog(store, guildId));
            }
        }
        const ok = verification.every((result) => result.ok);
        console.log(JSON.stringify({
            ok,
            compacted: true,
            db: path.resolve(db),
            start: start ?? null,
            end: end ?? null,
            ms: Date.now() - startedAt,
            verification
        }, null, 2));
        if (!ok) process.exitCode = 1;
    } finally {
        await store.close();
    }
}

async function runCompareHeads() {
    const guildId = argValue("guild");
    const relaySpec = argValue("relays");
    if (!guildId || !relaySpec) throw new Error("compare-heads requires --guild <id> --relays <ws://a,ws://b>");
    const relays = relaySpec.split(",").map((relay) => relay.trim()).filter(Boolean);
    const minValidHeads = positiveIntegerArg("min-valid-heads", Math.max(1, Math.ceil(relays.length / 2)));
    const minCanonicalCount = positiveIntegerArg("min-canonical-count", minValidHeads);
    const requireNoConflicts = !hasFlag("allow-conflicts");
    const client = new CgpClient({ relays });
    try {
        await client.connect();
        const heads = await client.getRelayHeads(guildId, { timeoutMs: positiveIntegerArg("timeout-ms", 10000), minHeads: 1 });
        const quorum = summarizeRelayHeadQuorum(guildId, heads);
        assertRelayHeadQuorum(guildId, heads, { minValidHeads, minCanonicalCount, requireNoConflicts });
        console.log(JSON.stringify({
            ok: true,
            relays,
            guildId,
            validHeads: quorum.validHeads.length,
            invalidHeads: quorum.invalidHeads.length,
            conflictCount: quorum.conflicts.length,
            canonical: quorum.canonical
        }, null, 2));
    } finally {
        client.close();
    }
}

function printHelp() {
    console.log(`CGP relay ops

Commands:
  verify-log    --db <leveldb-path> [--guild <guild-id>]
  export-guild  --db <leveldb-path> --guild <guild-id> --output <file>
  backup-db     --db <leveldb-path> --output <file>
  restore-db    --db <leveldb-path> --input <file>
  backup-jsonl  --db <leveldb-path> --output <file>
  restore-jsonl --db <leveldb-path> --input <file>
  sync-from-relay --db <leveldb-path> --guild <guild-id> --relay <ws://relay> (--expected-end-seq N --expected-end-hash HASH | --trust-source-head) [--limit N] [--private-key-hex HEX]
  repair-from-quorum --db <leveldb-path> --guild <guild-id> --relays <ws://relay-a,ws://relay-b> [--min-valid-heads N] [--min-canonical-count N] [--allow-conflicts] [--limit N] [--private-key-hex HEX]
  repair-indexes --db <leveldb-path> [--guild <guild-id>]
  compact-db   --db <leveldb-path> [--start KEY] [--end KEY] [--verify]
  compare-heads --guild <guild-id> --relays <ws://relay-a,ws://relay-b> [--min-valid-heads N] [--min-canonical-count N]
`);
}

async function main() {
    switch (command()) {
        case "verify-log":
            return await runVerifyLog();
        case "export-guild":
            return await runExportGuild();
        case "backup-db":
            return await runBackupDb();
        case "restore-db":
            return await runRestoreDb();
        case "backup-jsonl":
            return await runBackupJsonl();
        case "restore-jsonl":
            return await runRestoreJsonl();
        case "sync-from-relay":
            return await runSyncFromRelay();
        case "repair-from-quorum":
            return await runRepairFromQuorum();
        case "repair-indexes":
            return await runRepairIndexes();
        case "compact-db":
            return await runCompactDb();
        case "compare-heads":
            return await runCompareHeads();
        case "help":
        case "--help":
        case "-h":
            return printHelp();
        default:
            throw new Error(`Unknown relay ops command: ${command()}`);
    }
}

void main()
    .catch((error) => {
        console.error(error);
        process.exitCode = 1;
    })
    .finally(() => {
        // Ops commands are one-shot CLIs. Force exit so transport/db handles from
        // failed remote probes cannot keep automation and disaster-recovery jobs hung.
        if (process.env.CGP_RELAY_OPS_NO_FORCE_EXIT !== "1") {
            setImmediate(() => process.exit(process.exitCode ?? 0));
        }
    });
