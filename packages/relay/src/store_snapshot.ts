import fs from 'node:fs/promises';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { Level } from 'level';
const MAX_BYTES = 256 * 1024 * 1024;
const digest = (bytes: Buffer | string) => createHash('sha256').update(bytes).digest('hex');
export async function backupRelayStore(source: string, output: string) {
    const dbPath = path.resolve(source), outputPath = path.resolve(output);
    if (outputPath === dbPath || outputPath.startsWith(dbPath + path.sep))
        throw Error('Snapshot output must be outside source DB');
    try {
        await fs.access(path.join(dbPath, "RESTORE-INCOMPLETE"));
        throw Error("Cannot back up an incomplete restore");
    }
    catch (e: any) {
        if (e.code !== "ENOENT")
            throw e;
    }
    await fs.access(dbPath);
    try {
        await fs.access(outputPath);
        throw Error('Snapshot output already exists');
    }
    catch (e: any) {
        if (e.code !== 'ENOENT')
            throw e;
    }
    const db = new Level<Buffer, Buffer>(dbPath, { createIfMissing: false, keyEncoding: "buffer", valueEncoding: "buffer" });
    const entries: [
        string,
        string
    ][] = [];
    let bytes = 0;
    try {
        await db.open();
        for await (const [rawKey, rawValue] of db.iterator()) {
            const key = new TextDecoder("utf-8", { fatal: true }).decode(rawKey), value = new TextDecoder("utf-8", { fatal: true }).decode(rawValue);
            if (!Buffer.from(key).equals(rawKey) || !Buffer.from(value).equals(rawValue))
                throw Error("Noncanonical UTF8 keyspace: use binary encrypted backup tool");
            bytes += rawKey.length + rawValue.length;
            if (bytes > MAX_BYTES)
                throw Error('Snapshot exceeds bounded 256MiB limit');
            entries.push([key, value]);
        }
    }
    finally {
        await db.close();
    }
    const snapshot = { format: 'cgp.relay-store.v1', source: dbPath, createdAt: new Date().toISOString(), records: entries.length, entries };
    const encoded = JSON.stringify(snapshot);
    if (Buffer.byteLength(encoded) > MAX_BYTES)
        throw Error('Encoded snapshot exceeds bounded 256MiB limit');
    await fs.mkdir(path.dirname(outputPath), { recursive: true });
    await fs.writeFile(outputPath, encoded, { flag: 'wx', mode: 0o600 });
    return { ok: true, source: dbPath, output: outputPath, records: entries.length, sha256: digest(encoded), scope: 'complete CGP UTF8 keyspace; excludes relay key, runtime configuration and external media' };
}
export async function restoreRelayStore(input: string, target: string, expectedSha256: string, expectedSource: string) {
    const inputPath = path.resolve(input), targetPath = path.resolve(target), sourcePath = path.resolve(expectedSource);
    if (!/^[a-f0-9]{64}$/i.test(expectedSha256))
        throw Error('External expected SHA256 required');
    if (targetPath === sourcePath)
        throw Error('Restore requires a distinct fresh target');
    const info = await fs.stat(inputPath);
    if (info.size > MAX_BYTES)
        throw Error('Snapshot exceeds bounded 256MiB limit');
    const bytes = await fs.readFile(inputPath);
    if (digest(bytes) !== expectedSha256.toLowerCase())
        throw Error('Snapshot digest mismatch');
    const snapshot = JSON.parse(bytes.toString('utf8'));
    if (snapshot.format !== 'cgp.relay-store.v1' || snapshot.source !== sourcePath || !Array.isArray(snapshot.entries) || snapshot.records !== snapshot.entries.length)
        throw Error('Snapshot format/source/count mismatch');
    const seen = new Set<string>();
    for (const entry of snapshot.entries) {
        if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== 'string' || typeof entry[1] !== 'string' || seen.has(entry[0]))
            throw Error('Malformed or duplicate snapshot record');
        seen.add(entry[0]);
    }
    // Reserve a fresh target before creating LevelDB; never merge safety state into an existing node.
    await fs.mkdir(path.dirname(targetPath), { recursive: true });
    await fs.mkdir(targetPath);
    const marker = path.join(targetPath, 'RESTORE-INCOMPLETE');
    await fs.writeFile(marker, JSON.stringify({ input: inputPath, sha256: expectedSha256, source: sourcePath }));
    const db = new Level<string, string>(targetPath);
    try {
        await db.open();
        await db.batch(snapshot.entries.map(([key, value]: [
            string,
            string
        ]) => ({ type: 'put' as const, key, value })), { sync: true });
        const actual: [
            string,
            string
        ][] = [];
        for await (const pair of db.iterator())
            actual.push(pair);
        const wanted = [...snapshot.entries].sort((a, b) => Buffer.compare(Buffer.from(a[0]), Buffer.from(b[0])));
        if (JSON.stringify(actual) !== JSON.stringify(wanted))
            throw Error('Restored record verification mismatch');
    }
    finally {
        await db.close();
    }
    await fs.unlink(marker);
    return { ok: true, input: inputPath, target: targetPath, source: sourcePath, sha256: expectedSha256.toLowerCase(), records: snapshot.records, scope: 'offline exact-store restore; node keys/config/media restored separately; old node must remain fenced' };
}
