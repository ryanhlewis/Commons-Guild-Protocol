import { it, expect } from 'vitest';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { generatePrivateKey, getPublicKey, sign, hashObject, computeEventId } from '@cgp/core';
import { validateJsonlBackup, withValidatedJsonlCopy } from '../../../scripts/backup-validation';
async function records() { const priv = generatePrivateKey(), author = getPublicKey(priv), createdAt = Date.now(), body = { type: 'GUILD_CREATE', guildId: 'g', name: 'Synthetic' }; const event: any = { seq: 0, prevHash: null, body, author, createdAt, signature: await sign(priv, hashObject({ body, author, createdAt })) }; event.id = computeEventId(event); return [{ type: 'backup-header', format: 'cgp.relay-backup-jsonl.v1' }, { type: 'guild-start', guildId: 'g' }, { type: 'event', guildId: 'g', event }, { type: 'guild-end', guildId: 'g', verification: { ok: true, events: 1, headSeq: 0, headHash: event.id } }, { type: 'backup-footer', ok: true, guilds: 1 }]; }
it.each(['valid', 'truncated', 'scope', 'footer-count', 'head', 'signature', 'duplicate-guild'])('archive preflight %s', async (kind) => {
    const rows: any[] = await records();
    if (kind === 'truncated')
        rows.pop();
    if (kind === 'scope')
        rows[2].guildId = 'other';
    if (kind === 'footer-count')
        rows[4].guilds = 2;
    if (kind === 'head')
        rows[3].verification.headHash = 'wrong';
    if (kind === 'signature')
        rows[2].event.signature = '00';
    if (kind === 'duplicate-guild')
        rows.splice(4, 0, rows[1]);
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'cgp-archive-'));
    const file = path.join(dir, 'backup.jsonl');
    await fs.writeFile(file, rows.map(r => JSON.stringify(r)).join('\n'));
    if (kind === 'valid')
        expect(await validateJsonlBackup(file)).toEqual({ guilds: 1 });
    else
        await expect(validateJsonlBackup(file)).rejects.toThrow();
});
it("uses the validated private copy even when caller source is replaced before import", async () => {
    const rows = await records();
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), "cgp-source-change-"));
    const input = path.join(directory, "source.jsonl");
    const original = rows.map(record => JSON.stringify(record)).join("\n");
    await fs.writeFile(input, original);
    let stagedPath = "";
    await withValidatedJsonlCopy(input, async (copy) => {
        stagedPath = copy;
        await fs.writeFile(input, "malicious replacement");
        expect(await fs.readFile(copy, "utf8")).toBe(original);
        expect(await validateJsonlBackup(copy)).toEqual({ guilds: 1 });
    });
    await expect(fs.access(stagedPath)).rejects.toThrow();
});
