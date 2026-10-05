import {test,expect} from 'vitest';
import {mkdtemp,rm,mkdir,writeFile,readFile,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {admissionWorkValid,solveAdmissionWork,admissionNetworkKey,generatePrivateKey,getPublicKey,sign,hashObject,directoryRegistrationPayload,DIRECTORY_LEASE_MS,DIRECTORY_GRACE_MS,type DeviceAuthorization} from '@cgp/core';
import {DirectoryService} from '@cgp/directory/src/index';
import {StorageAdmission,type StorageAdmissionPolicy} from '@cgp/relay/src/storage_admission';
import {DeviceAuthorityRegistry} from '@cgp/core';

const policy:StorageAdmissionPolicy={totalBytes:100000,publisherBytes:20,pinBytes:12,publisherPinBytes:8,minimumFreeBytes:0,maxConcurrent:1,requestsPerMinute:2,networkRequestsPerMinute:3,globalRequestsPerMinute:10};
test('confirmed missing cache charge reconciliation preserves concurrent recreated files',async()=>fixture(async root=>{
 const admission=new StorageAdmission(root,policy),relative='pending-upload-blobs/'+ 'a'.repeat(64);
 await admission.run('alice',relative,4,Date.now()+86400000,false,async()=>{await mkdir(join(root,'pending-upload-blobs'));await writeFile(join(root,relative),'data');});
 await admission.forgetMissingFiles([relative]);
 expect(JSON.parse(await readFile(join(root,'admission-ledger.json'),'utf8')).some(([key]:[string])=>key===relative)).toBe(true);
 await rm(join(root,relative));await admission.forgetMissingFiles([relative]);
 expect(JSON.parse(await readFile(join(root,'admission-ledger.json'),'utf8')).some(([key]:[string])=>key===relative)).toBe(false);
 await expect(admission.forgetMissingFiles(['../outside'])).rejects.toThrow();
}));
test('cleanup performs one full untracked-inclusive audit and derives exact nested ledger charges',async()=>fixture(async root=>{
 await mkdir(join(root,'tree','child'),{recursive:true});await mkdir(join(root,'tree','expired'));
 await writeFile(join(root,'tree','child','data'),Buffer.alloc(3));await writeFile(join(root,'tree','expired','data'),Buffer.alloc(4));await writeFile(join(root,'untracked'),Buffer.alloc(6));
 const charge=(owner:string,expiresAt=100)=>({owner,bytes:999,expiresAt,pinned:false});
 await writeFile(join(root,'admission-ledger.json'),JSON.stringify([['tree',charge('alice')],['tree/child',charge('bob')],['tree/expired',charge('carol',0)],['missing',charge('dave')]]));
 const admission=new StorageAdmission(root,policy),calls:string[]=[],walk=(admission as any).bytes.bind(admission);
 (admission as any).bytes=(target:string,...args:any[])=>{calls.push(target);return walk(target,...args);};
 await admission.cleanup(1);
 const ledger=JSON.parse(await readFile(join(root,'admission-ledger.json'),'utf8'));
 expect(ledger.map(([key,value]:any)=>[key,value.owner,value.bytes])).toEqual([['tree','alice',3],['tree/child','bob',3]]);
 expect(calls.filter(p=>p===root)).toHaveLength(1);expect(new Set(calls).size).toBe(calls.length);
 expect(admission.cleanupAuditSummary()).toMatchObject({entries:calls.length,records:4});expect(admission.cleanupAuditSummary()!.bytes).toBeGreaterThanOrEqual(13);
 expect((await readFile(join(root,'untracked'))).length).toBe(6);
}));
test('cleanup rejects symlinks and the global file-count boundary before deleting expired storage',async()=>fixture(async root=>{
 await mkdir(join(root,'safe'));await writeFile(join(root,'safe','data'),Buffer.alloc(2));
 const ledger=JSON.stringify([['safe',{owner:'alice',bytes:2,expiresAt:0,pinned:false}]]);await writeFile(join(root,'admission-ledger.json'),ledger);
 const limited=new StorageAdmission(root,{...policy,maxScanEntries:2});await expect(limited.cleanup(1)).rejects.toThrow('file-count budget');expect(await readFile(join(root,'admission-ledger.json'),'utf8')).toBe(ledger);expect((await readFile(join(root,'safe','data'))).length).toBe(2);
 await symlink(join(root,'safe'),join(root,'untracked-link'),process.platform==='win32'?'junction':'dir');
 await expect(new StorageAdmission(root,policy).cleanup(1)).rejects.toThrow('Symlinks');expect(await readFile(join(root,'admission-ledger.json'),'utf8')).toBe(ledger);expect((await readFile(join(root,'safe','data'))).length).toBe(2);
}));
test('storage audit entry budget is operator configurable and remains bounded',async()=>fixture(async root=>{
 await mkdir(join(root,'untracked'));
 await writeFile(join(root,'untracked','one'),Buffer.alloc(3));
 await writeFile(join(root,'untracked','two'),Buffer.alloc(4));
 const defaults=new StorageAdmission(root,policy);
 expect(defaults.policy.maxScanEntries).toBe(200000);
 expect(new StorageAdmission(root,{...policy,maxScanEntries:undefined}).policy.maxScanEntries).toBe(200000);
 await expect((defaults as any).bytes(root,{files:199998})).rejects.toThrow('file-count budget');
 const expanded=new StorageAdmission(root,{...policy,maxScanEntries:2000000});
 expect(await (expanded as any).bytes(root,{files:199998})).toBe(7);
 const limited=new StorageAdmission(root,{...policy,maxScanEntries:3});
 await expect(limited.run('alice','new',1,Date.now()+10000,false,async()=>{})).rejects.toMatchObject({status:507,message:'Storage file-count budget reached'});
 for(const maxScanEntries of [0,-1,1.5,2000001,NaN,Infinity])expect(()=>new StorageAdmission(root,{...policy,maxScanEntries})).toThrow('audit entry budget');
 // Raising the traversal allowance never hides existing untracked bytes.
 const quota=new StorageAdmission(root,{...policy,maxScanEntries:2000000,totalBytes:7});
 await expect(quota.run('alice','new',1,Date.now()+10000,false,async()=>{})).rejects.toThrow('quota');
}));
test('parallel storage audit counts nested untracked bytes and rejects directory links',async()=>fixture(async root=>{
 const admission=new StorageAdmission(root,policy);
 await Promise.all(Array.from({length:8},async(_,dir)=>{
  const folder=join(root,'untracked-'+dir);await mkdir(folder);
  await Promise.all(Array.from({length:16},(_,file)=>writeFile(join(folder,String(file)),Buffer.alloc(file+1))));
 }));
 expect(await (admission as any).bytes(root)).toBe(8*136);
 const limited=new StorageAdmission(root,{...policy,totalBytes:100});
 await expect(limited.run('alice','new',1,Date.now()+10000,false,async()=>{})).rejects.toThrow('quota');
 await symlink(join(root,'untracked-0'),join(root,'linked'),process.platform==='win32'?'junction':'dir');
 await expect((admission as any).bytes(root)).rejects.toThrow('Symlinks');
}));
test('batch admission audits the root once and durably charges every failed partial write',async()=>fixture(async root=>{
 const admission=new StorageAdmission(root,{...policy,publisherBytes:30});
 const original=(admission as any).bytes.bind(admission);let rootAudits=0;
 (admission as any).bytes=async(target:string,...args:any[])=>{if(target===root)rootAudits++;return original(target,...args);};
 const entries=[{relative:'one',reservation:7,expiresAt:Date.now()+10000,pinned:false},{relative:'two',reservation:8,expiresAt:Date.now()+20000,pinned:false}];
 await expect(admission.runBatch('alice',entries,async()=>{await writeFile(join(root,'one'),Buffer.alloc(7));await writeFile(join(root,'two'),Buffer.alloc(8));throw Error('batch interrupted');})).rejects.toThrow('batch interrupted');
 expect(rootAudits).toBe(1);
 const ledger=JSON.parse(await readFile(join(root,'admission-ledger.json'),'utf8'));expect(ledger.map(([key,value]:any)=>[key,value.owner,value.bytes])).toEqual([['one','alice',7],['two','alice',8]]);
 const resumed=new StorageAdmission(root,{...policy,publisherBytes:30});
 await expect(resumed.runBatch('alice',[{relative:'three',reservation:16,expiresAt:Date.now()+10000,pinned:false}],async()=>{})).rejects.toThrow('quota');
 await expect(resumed.runBatch('bob',entries,async()=>{})).rejects.toThrow('another publisher');
}));
test('batch validation rejects cumulative overages, overlapping paths, symlinks and oversized batches before writes',async()=>fixture(async root=>{
 const admission=new StorageAdmission(root,policy),entry=(relative:string,reservation=11)=>({relative,reservation,expiresAt:Date.now()+10000,pinned:false});let writes=0;
 const operation=async()=>{writes++;};
 await expect(admission.runBatch('alice',[entry('one'),entry('two')],operation)).rejects.toThrow('quota');
 await expect(admission.runBatch('alice',[entry('one'),entry('one/child')],operation)).rejects.toThrow('overlap');
 await expect(admission.runBatch('alice',[entry('one'),entry('alias/../one')],operation)).rejects.toThrow('overlap');
 await expect(admission.runBatch('alice',Array.from({length:65},(_,index)=>entry(String(index))),operation)).rejects.toThrow('1 to 64');
 await mkdir(join(root,'actual'));await symlink(join(root,'actual'),join(root,'linked'),process.platform==='win32'?'junction':'dir');
 await expect(admission.runBatch('alice',[entry('linked/file',1)],operation)).rejects.toThrow('Symlinks');expect(writes).toBe(0);
}));
test('successful batches preserve each expiry and aggregate only pinned reservations',async()=>fixture(async root=>{
 const admission=new StorageAdmission(root,policy);
 await admission.runBatch('alice',[{relative:'pin',reservation:7,expiresAt:10,pinned:true},{relative:'plain',reservation:10,expiresAt:20,pinned:false}],async()=>{await writeFile(join(root,'pin'),Buffer.alloc(7));await writeFile(join(root,'plain'),Buffer.alloc(10));});
 expect(admission.retention('pin')).toEqual({expiresAt:10,pinned:true});expect(admission.retention('plain')).toEqual({expiresAt:20,pinned:false});
 await expect(admission.runBatch('alice',[{relative:'pin2',reservation:2,expiresAt:30,pinned:true}],async()=>{})).rejects.toThrow('pinning budget');
 await admission.cleanup(21);expect((await readFile(join(root,'pin'))).length).toBe(7);await expect(readFile(join(root,'plain'))).rejects.toThrow();
}));
test('restoring a newer device generation may reset its epoch but cannot restore an older authority',()=>{
 const registry=new DeviceAuthorityRegistry(),accountPublicKey=getPublicKey(generatePrivateKey()),authorityPublicKey=getPublicKey(generatePrivateKey());
 const old={accountPublicKey,authorityPublicKey,generation:1,activatedAt:1,revocationEpoch:10,observedAt:1};registry.restoreTrustedPin(old);
 const current={...old,authorityPublicKey:getPublicKey(generatePrivateKey()),generation:2,revocationEpoch:0,observedAt:2};registry.restoreTrustedPin(current);
 expect(registry.get(accountPublicKey)?.generation).toBe(2);registry.restoreTrustedPin(old);expect(registry.get(accountPublicKey)?.authorityPublicKey).toBe(current.authorityPublicKey);
});
async function fixture(run:(root:string)=>Promise<void>){const root=await mkdtemp(join(tmpdir(),'cgp-admission-'));try{await run(root);}finally{await rm(root,{recursive:true,force:true});}}
test('work is bounded and bound to the exact task; networks aggregate IPv4 and IPv6',async()=>{
 const nonce=await solveAdmissionWork('task',8);expect(admissionWorkValid('task',nonce,8)).toBe(true);
 expect(admissionWorkValid('task',nonce,24)).toBe(false);expect(admissionWorkValid('task','invalid',0)).toBe(false);
 await expect(solveAdmissionWork('task',21)).rejects.toThrow('budget');
 const cancelled=new AbortController();cancelled.abort();await expect(solveAdmissionWork('task',8,cancelled.signal)).rejects.toThrow();
 expect(admissionNetworkKey('::ffff:192.0.2.1')).toBe(admissionNetworkKey('192.0.2.254'));
 expect(admissionNetworkKey('2001:db8:1:2::1')).toBe(admissionNetworkKey('2001:0db8:0001:0002:ffff::2'));
});
test('partial writes remain charged across restart; quotas are cumulative and serialized',async()=>fixture(async root=>{
 let admission=new StorageAdmission(root,policy);
 await expect(admission.run('alice','partial',12,Date.now()+10000,false,async()=>{await writeFile(join(root,'partial'),Buffer.alloc(12));throw new Error('interrupted');})).rejects.toThrow('interrupted');
 admission=new StorageAdmission(root,policy);
 await expect(admission.run('alice','second',9,Date.now()+10000,false,async()=>{})).rejects.toThrow('quota');
 const requests=['one','two'].map(name=>admission.run('bob',name,12,Date.now()+10000,false,()=>writeFile(join(root,name),Buffer.alloc(12))));
 const outcomes=await Promise.allSettled(requests);expect(outcomes.filter(x=>x.status==='fulfilled')).toHaveLength(1);
 await expect(admission.run('mallory','partial',0,Date.now()+10000,false,async()=>{})).rejects.toThrow('another publisher');
}));
test('retention removes expired staging, keeps pins and owner-only renewals; pin and disk budgets fail closed',async()=>fixture(async root=>{
 const admission=new StorageAdmission(root,policy);
 await admission.run('alice','pending',6,10,false,()=>writeFile(join(root,'pending'),Buffer.alloc(6)));
 await admission.run('alice','pin',7,10,true,()=>writeFile(join(root,'pin'),Buffer.alloc(7)));
 await expect(admission.run('alice','pin2',2,100,true,async()=>{})).rejects.toThrow('pinning budget');
 await admission.renew('mallory','pending',1000);await admission.cleanup(11);
 await expect(readFile(join(root,'pending'))).rejects.toThrow();expect((await readFile(join(root,'pin'))).length).toBe(7);
 await admission.run('bob','renewed',5,20,false,()=>writeFile(join(root,'renewed'),Buffer.alloc(5)));
 await admission.renew('bob','renewed',1000);await admission.cleanup(21);expect((await readFile(join(root,'renewed'))).length).toBe(5);
 await expect(admission.run('bob','../escape',1,100,false,async()=>{})).rejects.toThrow('escapes');
 const disk=new StorageAdmission(root,{...policy,minimumFreeBytes:Number.MAX_SAFE_INTEGER});await expect(disk.run('bob','disk',1,100,false,async()=>{})).rejects.toThrow('watermark');
}));
test('concurrency and network admission bound many identities behind the same network',()=>{
 const admission=new StorageAdmission('.',policy);
 const req=(address:string)=>({headers:{},socket:{remoteAddress:address}} as any);
 const leave=admission.enter(req('192.0.2.1'));expect(()=>admission.enter(req('192.0.2.2'))).toThrow('concurrency');leave();leave();
 admission.enter(req('192.0.2.3'))();expect(()=>admission.enter(req('192.0.2.4'))).toThrow('rate');
 for(let i=0;i<3;i++)admission.admitPublisher('alice');expect(()=>admission.admitPublisher('alice')).toThrow('Publisher');
});
test('directory challenge, aliases, grace and reclaim change only the binding, never the key/history',async()=>fixture(async root=>{
 const service=new DirectoryService(join(root,'db'),{workBits:8,maxHandles:2});
 const alice=generatePrivateKey(),bob=generatePrivateKey(),pub=getPublicKey(alice),other=getPublicKey(bob);
 const register=async(handle:string,key:Uint8Array,work=true)=>{const timestamp=Date.now(),publicKey=getPublicKey(key),payload=directoryRegistrationPayload(handle,`profile:${publicKey}`,publicKey,timestamp,[]);const signature=await sign(key,hashObject(payload));return service.register(handle,`profile:${publicKey}`,publicKey,signature,timestamp,[],undefined,work?await solveAdmissionWork(hashObject(payload),8):undefined);};
 try {
  await expect(register('alice',alice,false)).rejects.toThrow('admission work');await register('alice',alice);await register('alias',alice);
  await expect(register('third',alice)).rejects.toThrow('quota');await expect(register('guest',bob)).rejects.toThrow('Reserved');
  const entry=(service as any).entries.get('alice');entry.leaseExpiresAt=Date.now()-1;entry.reclaimAfter=Date.now()+10000;
  await expect(register('alice',bob)).rejects.toThrow('owned');entry.reclaimAfter=Date.now()-1;
  await register('alice',bob);const reclaimed=(await service.getEntry('alice'))!;
  expect(reclaimed.guildPubkey).toBe(other);expect(reclaimed.guildId).toBe(`profile:${other}`);expect((await service.getEntry('alias'))!.guildPubkey).toBe(pub);
  expect(reclaimed.reclaimAfter!-reclaimed.leaseExpiresAt!).toBe(DIRECTORY_GRACE_MS);
  expect(reclaimed.leaseExpiresAt!-reclaimed.registeredAt!).toBe(DIRECTORY_LEASE_MS);
 }finally{await service.close();}
}));
test('reclaim and restart preserve the former owner’s device authority pin',async()=>fixture(async root=>{
 const db=join(root,'db'),account=generatePrivateKey(),authority=generatePrivateKey(),device=generatePrivateKey(),now=Date.now();
 const accountPublicKey=getPublicKey(account),authorityPublicKey=getPublicKey(authority),devicePublicKey=getPublicKey(device);
 const binding={protocol:'cgp/device-authority/1' as const,accountPublicKey,authorityPublicKey,generation:1,activatedAt:now-1000};
 const certificate={protocol:'cgp/device-certificate/1' as const,accountPublicKey,authorityPublicKey,devicePublicKey,serial:'ab'.repeat(16),label:'Device',capabilities:['publish' as const],issuedAt:now-500,expiresAt:now+60000};
 const revocation={protocol:'cgp/device-revocation/1' as const,accountPublicKey,authorityPublicKey,generation:1,epoch:0,updatedAt:now-100,revokedSerials:[]};
 const authorization:DeviceAuthorization={protocol:'cgp/device-authorization/1',binding:{...binding,signature:await sign(account,hashObject(binding))},certificate:{...certificate,signature:await sign(authority,hashObject(certificate))},revocation:{...revocation,signature:await sign(authority,hashObject(revocation))}};
 let service=new DirectoryService(db,{workBits:0});
 try {
  const payload=directoryRegistrationPayload('alice','old-profile',accountPublicKey,now,[]);
  await service.register('alice','old-profile',accountPublicKey,await sign(device,hashObject({payload,deviceAuthorization:authorization})),now,[],authorization);
  const old=(service as any).entries.get('alice');old.leaseExpiresAt=now-2;old.reclaimAfter=now-1;
  const bob=generatePrivateKey(),pub=getPublicKey(bob),time=Date.now()+1,p=directoryRegistrationPayload('alice','new-profile',pub,time,[]);
  await service.register('alice','new-profile',pub,await sign(bob,hashObject(p)),time,[]);
  await service.close();service=new DirectoryService(db,{workBits:0});
  const time2=Date.now()+2,direct=directoryRegistrationPayload('another','old-profile',accountPublicKey,time2,[]);
  await expect(service.register('another','old-profile',accountPublicKey,await sign(account,hashObject(direct)),time2,[])).rejects.toThrow('disabled after device authority activation');
  expect((await service.getEntry('alice'))!.guildPubkey).toBe(pub);
 }finally{await service.close();}
}));
