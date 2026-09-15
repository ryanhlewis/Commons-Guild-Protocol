import { test, expect } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Level } from 'level';
import { DirectoryService, verifyDirectoryLookupProof } from '@cgp/directory/src/index';
import { generatePrivateKey, getPublicKey, sign, hashObject, directoryRegistrationPayload } from '@cgp/core';

async function fixture(run: (service: DirectoryService) => Promise<void>) {
    const root = await mkdtemp(join(tmpdir(), 'cgp-registration-'));
    const service = new DirectoryService(join(root, 'db'));
    try { await run(service); } finally { await service.close(); await rm(root, {recursive:true,force:true}); }
}
async function registration(key = generatePrivateKey(), timestamp = Date.now(), relays = ['wss://relay.example']) {
    const pub = getPublicKey(key);
    return { key, pub, timestamp, relays, signature: await sign(key, hashObject(directoryRegistrationPayload('alice','guild',pub,timestamp,relays))) };
}
const submit = (service: DirectoryService, r: Awaited<ReturnType<typeof registration>>) => service.register('alice','guild',r.pub,r.signature,r.timestamp,r.relays);

test('preserves legacy ownership and v2 registrations across database restarts', async () => {
    const root = await mkdtemp(join(tmpdir(), 'cgp-migration-'));
    const path = join(root, 'db');
    const a = await registration();
    const db = new Level(path);
    await db.put('alice', JSON.stringify({handle:'alice',guildId:'guild',guildPubkey:a.pub,registeredAt:a.timestamp-1,relays:a.relays,registrationSignature:await sign(a.key,hashObject(`REGISTER:alice:guild:${a.timestamp-1}`))}));
    await db.close();
    let service = new DirectoryService(path);
    try {
        expect((await service.getEntry('alice'))!.registrationVersion).toBeUndefined();
        await expect(submit(service,await registration())).rejects.toThrow('owned');
        await submit(service,a);
        await service.close();
        service = new DirectoryService(path);
        const lookup = (await service.getLookupProof('alice'))!;
        expect(lookup.entry.registrationVersion).toBe(2);
        expect(lookup.entry.guildPubkey).toBe(a.pub);
        expect(verifyDirectoryLookupProof(lookup,{trustedOperatorPubkeys:[service.operatorPubkey]})).toBe(true);
        await expect(submit(service,await registration())).rejects.toThrow('owned');
    } finally { await service.close(); await rm(root,{recursive:true,force:true}); }
});

test('preserves ownership under sequential and simultaneous handle claims', async () => fixture(async service => {
    const a = await registration(), b = await registration();
    const results = await Promise.allSettled([submit(service,a),submit(service,b)]);
    expect(results.filter(r=>r.status==='fulfilled')).toHaveLength(1);
    const owner = (await service.getEntry('alice'))!.guildPubkey;
    await expect(submit(service,owner===a.pub?b:a)).rejects.toThrow('owned');
    expect((await service.getEntry('alice'))!.guildPubkey).toBe(owner);
}));

test('binds relays, rejects legacy writes/replays, and permits idempotent retries and newer owner updates', async () => fixture(async service => {
    const a = await registration(); await submit(service,a); await submit(service,a);
    await expect(submit(service,{...a,relays:['wss://attacker.example']})).rejects.toThrow('Invalid signature');
    const legacy = await sign(a.key,hashObject(`REGISTER:alice:guild:${a.timestamp}`));
    await expect(submit(service,{...a,signature:legacy})).rejects.toThrow('Invalid signature');
    const newer = await registration(a.key,a.timestamp+1,['wss://new.example']); await submit(service,newer);
    await expect(submit(service,a)).rejects.toThrow('newer');
    expect((await service.getEntry('alice'))!.relays).toEqual(newer.relays);
}));

test('publishes a consistent snapshot during writes and preserves it on database failure', async () => fixture(async service => {
    const a = await registration(); await submit(service,a);
    const db = (service as any).db;
    const put = db.put.bind(db);
    let release!:()=>void, entered!:()=>void;
    const paused = new Promise<void>(resolve=>{entered=resolve;});
    const resume = new Promise<void>(resolve=>{release=resolve;});
    db.put = async (...args:unknown[]) => { entered(); await resume; await put(...args); };
    const next = await registration(a.key,a.timestamp+1,['wss://next.example']);
    const writing = submit(service,next); await paused;
    try {
        const lookup = (await service.getLookupProof('alice'))!;
        expect(lookup.entry.relays).toEqual(a.relays);
        expect(verifyDirectoryLookupProof(lookup,{trustedOperatorPubkeys:[service.operatorPubkey]})).toBe(true);
    } finally { release(); await writing; }
    const lookup = (await service.getLookupProof('alice'))!;
    expect(lookup.entry.relays).toEqual(next.relays);
    expect(verifyDirectoryLookupProof(lookup,{trustedOperatorPubkeys:[service.operatorPubkey]})).toBe(true);
    lookup.entry.relays!.push('wss://mutated.example');
    expect((await service.getEntry('alice'))!.relays).toEqual(next.relays);
    db.put = async()=>{throw new Error('disk failure');};
    await expect(submit(service,await registration(a.key,a.timestamp+2))).rejects.toThrow('disk failure');
    expect((await service.getEntry('alice'))!.relays).toEqual(next.relays);
    db.put = put;
    await submit(service,await registration(a.key,a.timestamp+3));
}));
