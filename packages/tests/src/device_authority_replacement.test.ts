import { expect, test } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DirectoryService, verifyDirectoryLookupProof } from '@cgp/directory/src/index';
import { RelayServer } from '@cgp/relay/src/server';
import WebSocket from 'ws';
import { DeviceAuthorityRegistry, generatePrivateKey, getPublicKey, sign, hashObject, directoryRegistrationPayload, type DeviceAuthorization, type DeviceAuthorityBinding } from '@cgp/core';

async function fixture() {
    const accountKey = generatePrivateKey(), account = getPublicKey(accountKey), deviceKey = generatePrivateKey(), device = getPublicKey(deviceKey);
    const signed = async <T extends object>(key: string, value: T) => ({ ...value, signature: await sign(key, hashObject(value)) });
    const firstKey = generatePrivateKey(), firstPub = getPublicKey(firstKey), now = Date.now() - 100;
    const firstBinding: DeviceAuthorityBinding = await signed(accountKey, {protocol:'cgp/device-authority/1' as const, accountPublicKey:account, authorityPublicKey:firstPub, generation:1, activatedAt:now});
    const auth = async (key: string, binding: DeviceAuthorityBinding, epoch: number): Promise<DeviceAuthorization> => ({
        protocol:'cgp/device-authorization/1', binding,
        certificate:await signed(key,{protocol:'cgp/device-certificate/1' as const,accountPublicKey:account,authorityPublicKey:binding.authorityPublicKey,devicePublicKey:device,serial:String(binding.generation).repeat(32),label:'Linked',capabilities:['publish','read'],issuedAt:binding.activatedAt,expiresAt:now+3600000}),
        revocation:await signed(key,{protocol:'cgp/device-revocation/1' as const,accountPublicKey:account,authorityPublicKey:binding.authorityPublicKey,generation:binding.generation,epoch,updatedAt:binding.activatedAt,revokedSerials:[]})
    });
    const first = await auth(firstKey,firstBinding,2);
    const nextKey = generatePrivateKey(), nextPub = getPublicKey(nextKey);
    const replacement = await signed(firstKey,{protocol:'cgp/device-authority-replacement/1' as const,accountPublicKey:account,previousAuthorityPublicKey:firstPub,authorityPublicKey:nextPub,previousGeneration:1,generation:2,previousRevocationEpoch:2,revocationEpoch:3,activatedAt:now+1});
    const binding = await signed(accountKey,{protocol:'cgp/device-authority/1' as const,accountPublicKey:account,authorityPublicKey:nextPub,generation:2,activatedAt:now+1,replacements:[replacement]});
    const next = await auth(nextKey,binding,3);
    const signPayload = (payload: unknown, authorization: DeviceAuthorization) => sign(deviceKey,hashObject({payload,deviceAuthorization:authorization}));
    return {account,accountKey,first,next,signPayload,signed};
}

test('pins authenticated authority continuity and rejects old generations, skipped continuity and altered links', async () => {
    const f = await fixture(), registry = new DeviceAuthorityRegistry(), payload = {content:'after replacement'};
    expect(registry.verify(payload,await f.signPayload(payload,f.first),f.account,f.first,'publish').ok).toBe(true);
    expect(registry.verify(payload,await f.signPayload(payload,f.next),f.account,f.next,'publish').ok).toBe(true);
    expect(registry.get(f.account)).toMatchObject({generation:2,revocationEpoch:3,activatedAt:f.first.binding.activatedAt});
    expect(registry.verify(payload,await f.signPayload(payload,f.first),f.account,f.first,'publish').ok).toBe(false);
    expect(registry.verify(payload,await sign(f.accountKey,hashObject(payload)),f.account,undefined,'publish').ok).toBe(false);
    for (const replacements of [undefined, [{...f.next.binding.replacements![0],previousRevocationEpoch:0}], [{...f.next.binding.replacements![0],signature:'00'.repeat(64)}]]) {
        const {signature: _,...unsigned} = f.next.binding;
        const bad = {...f.next,binding:await f.signed(f.accountKey,{...unsigned,replacements})};
        expect(new DeviceAuthorityRegistry().verify(payload,await f.signPayload(payload,bad),f.account,bad,'publish').ok).toBe(false);
    }
});

test('delegated directory re-registration carries replacement continuity and persists rejection of old authority across restart', async () => {
    const f = await fixture(), root = await mkdtemp(join(tmpdir(),'cgp-authority-directory-')), path = join(root,'db');
    let service = new DirectoryService(path);
    let timestamp = Date.now();
    const submit = async (authorization: DeviceAuthorization, relays = ['wss://relay.example']) => {
        const time = ++timestamp, payload = directoryRegistrationPayload('alice','profile',f.account,time,relays);
        return service.register('alice','profile',f.account,await f.signPayload(payload,authorization),time,relays,authorization);
    };
    try {
        await submit(f.first); await submit(f.next);
        const proof = (await service.getLookupProof('alice'))!;
        expect(proof.entry.registrationVersion).toBe(3);
        expect(verifyDirectoryLookupProof(proof,{trustedOperatorPubkeys:[service.operatorPubkey]})).toBe(true);
        await expect(submit(f.first)).rejects.toThrow();
        await service.close(); service = new DirectoryService(path);
        await expect(submit(f.first)).rejects.toThrow();
        await submit(f.next,['wss://replacement.example']);
        expect((await service.getEntry('alice'))?.relays).toEqual(['wss://replacement.example']);
        const time = ++timestamp;
        await expect(service.register('alice','profile',f.account,await sign(f.accountKey,hashObject(directoryRegistrationPayload('alice','profile',f.account,time,[]))),time,[])).rejects.toThrow();
    } finally { await service.close(); await rm(root,{recursive:true,force:true}); }
});

test('real relay persists account-wide replacement fences across restart and refuses a predecessor in another guild', async () => {
    const f = await fixture(), root = await mkdtemp(join(tmpdir(),'cgp-authority-relay-'));
    let relay = new RelayServer(0,join(root,'db'),[],{enableDefaultPlugins:false});
    let socket: WebSocket | undefined;
    const open = async () => {
        const deadline = Date.now()+5000;
        while (!Number.isFinite(relay.getPort())) { if (Date.now()>deadline) throw new Error('Relay startup timeout'); await new Promise(r=>setTimeout(r,10)); }
        socket = new WebSocket(`ws://localhost:${relay.getPort()}`);
        await new Promise<void>((resolve,reject)=>{socket!.once('open',resolve);socket!.once('error',reject);});
    };
    const publish = async (body: object, authorization: DeviceAuthorization) => {
        const payload = {body,author:f.account,createdAt:Date.now()}, clientEventId = crypto.randomUUID();
        const signature = await f.signPayload(payload,authorization);
        return await new Promise<[string,{seq?:number;code?:string}]>((resolve,reject)=>{
            const timeout = setTimeout(()=>{socket!.off('message',receive);reject(new Error('Relay reply timeout'));},5000);
            const receive = (raw: WebSocket.RawData) => { const frame = JSON.parse(raw.toString()); if (frame[1]?.clientEventId === clientEventId) {clearTimeout(timeout);socket!.off('message',receive);resolve(frame);} };
            socket!.on('message',receive); socket!.send(JSON.stringify(['PUBLISH',{...payload,signature,deviceAuthorization:authorization,clientEventId}]));
        });
    };
    try {
        await open();
        const a = hashObject({name:'old-guild',account:f.account}), profile = hashObject({name:'profile',account:f.account});
        expect(await publish({type:'GUILD_CREATE',guildId:a,name:'Existing guild',access:'public'},f.first)).toMatchObject(['PUB_ACK',{seq:0}]);
        expect(await publish({type:'GUILD_CREATE',guildId:profile,name:'New profile authority',access:'public'},f.next)).toMatchObject(['PUB_ACK',{seq:0}]);
        socket!.terminate(); await relay.close();
        relay = new RelayServer(0,join(root,'db'),[],{enableDefaultPlugins:false}); await open();
        const channel = {type:'CHANNEL_CREATE',guildId:a,channelId:'after-restart',name:'After restart',kind:'text'};
        expect(await publish(channel,f.first)).toMatchObject(['ERROR',{code:'INVALID_SIGNATURE'}]);
        expect(await publish(channel,f.next)).toMatchObject(['PUB_ACK',{seq:1}]);
    } finally { socket?.terminate(); await relay.close(); await rm(root,{recursive:true,force:true}); }
});
