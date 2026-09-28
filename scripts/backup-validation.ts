import promises from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import fs from 'node:fs';
import readline from 'node:readline';
import { computeEventId, hashObject, verify, verifyDeviceAuthorizedObject, type GuildEvent } from '@cgp/core';
export function verifyArchiveEvent(event: GuildEvent, previous?: GuildEvent) {
    const errors: string[] = [];
    try {
        if (event.seq !== (previous ? previous.seq + 1 : 0) || event.prevHash !== (previous ? previous.id : null))
            errors.push('sequence/hash-chain mismatch');
        if (!previous && event.body.type !== 'GUILD_CREATE')
            errors.push('genesis must be GUILD_CREATE');
        if (computeEventId(event) !== event.id)
            errors.push('event id mismatch');
        const payload = { body: event.body, author: event.author, createdAt: event.createdAt };
        const valid = event.deviceAuthorization ? verifyDeviceAuthorizedObject(payload, event.signature, event.deviceAuthorization, { accountPublicKey: event.author, requiredCapability: 'publish', now: event.createdAt }).ok : verify(event.author, hashObject(payload), event.signature);
        if (!valid)
            errors.push('signature mismatch');
    }
    catch {
        errors.push('malformed event');
    }
    return errors;
}
export async function validateJsonlBackup(input: string) {
    let header = false, footer = false, active: string | undefined, previous: GuildEvent | undefined, count = 0;
    const completed = new Set<string>();
    const lines = readline.createInterface({ input: fs.createReadStream(input, { encoding: 'utf8' }), crlfDelay: Infinity });
    for await (const line of lines) {
        if (!line.trim())
            continue;
        const record = JSON.parse(line);
        if (footer)
            throw Error('Records after backup footer');
        if (!header) {
            if (record.type !== 'backup-header' || record.format !== 'cgp.relay-backup-jsonl.v1')
                throw Error('Missing or invalid backup header');
            header = true;
            continue;
        }
        if (record.type === 'guild-start') {
            if (active || typeof record.guildId !== 'string' || !record.guildId || completed.has(record.guildId))
                throw Error('Invalid or duplicate guild start');
            active = record.guildId;
            previous = undefined;
            count = 0;
        }
        else if (record.type === 'event') {
            if (!active || record.guildId !== active || record.event?.body?.guildId !== active)
                throw Error('Backup event guild scope mismatch');
            const errors = verifyArchiveEvent(record.event, previous);
            if (errors.length)
                throw Error(`Invalid backup event: ${errors.join('; ')}`);
            previous = record.event;
            count++;
        }
        else if (record.type === 'guild-end') {
            if (!active || record.guildId !== active || record.verification?.ok !== true || record.verification.events !== count || record.verification.headSeq !== (previous?.seq ?? -1) || record.verification.headHash !== (previous?.id ?? null))
                throw Error('Backup guild end verification mismatch');
            completed.add(active);
            active = undefined;
        }
        else if (record.type === 'backup-footer') {
            if (active || record.ok !== true || record.guilds !== completed.size)
                throw Error('Backup footer verification mismatch');
            footer = true;
        }
        else
            throw Error('Unknown backup record');
    }
    if (!header || !footer || active)
        throw Error('Incomplete backup: header/footer/guild end required');
    return { guilds: completed.size };
}
export async function withValidatedJsonlCopy<T>(input: string, consume: (copy: string) => Promise<T>): Promise<T> {
    const directory = await promises.mkdtemp(path.join(os.tmpdir(), "cgp-validated-archive-"));
    const copy = path.join(directory, "archive.jsonl");
    try {
        // Copy first: subsequent replacement of the caller's source cannot change import bytes.
        await promises.copyFile(input, copy, fs.constants.COPYFILE_EXCL);
        await promises.chmod(copy, 0o400);
        await validateJsonlBackup(copy);
        return await consume(copy);
    }
    finally {
        await promises.chmod(copy, 0o600).catch(() => undefined);
        await promises.unlink(copy).catch(error => { if (error.code !== "ENOENT")
            throw error; });
        await promises.rmdir(directory);
    }
}
