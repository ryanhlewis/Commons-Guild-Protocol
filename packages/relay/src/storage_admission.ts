import { mkdir, readdir, lstat, readFile, writeFile, rename, rm, statfs } from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import type { IncomingMessage } from 'node:http';
import { admissionNetworkKey } from '@cgp/core';

export interface StorageAdmissionPolicy {
 totalBytes: number; publisherBytes: number; pinBytes: number; publisherPinBytes: number;
 minimumFreeBytes: number; maxConcurrent: number; requestsPerMinute: number; networkRequestsPerMinute: number; globalRequestsPerMinute: number;
}
interface Charge { owner: string; bytes: number; expiresAt: number; pinned: boolean }
export class AdmissionError extends Error { constructor(message: string, public status = 429) { super(message); } }

/** Operator-local accounting. Published identity/log ownership is never erased by retention. */
export class StorageAdmission {
 private queue: Promise<unknown> = Promise.resolve();
 private records = new Map<string, Charge>();
 private ready: Promise<void> | null = null;
 private active = 0;
 private rates = new Map<string, { minute: number; count: number }>();
 constructor(private root: string, readonly policy: StorageAdmissionPolicy, private trustProxy = false) {
  this.root = path.resolve(root);
  if(Object.values(policy).some(value=>!Number.isSafeInteger(value)||value<0)||policy.maxConcurrent<1||policy.requestsPerMinute<1||policy.networkRequestsPerMinute<1||policy.globalRequestsPerMinute<1)throw new Error('Invalid storage admission policy');
 }
 private async load() {
  this.ready ??= (async () => {
   await mkdir(this.root, {recursive:true});
   try {
    const rows = JSON.parse(await readFile(path.join(this.root, 'admission-ledger.json'), 'utf8'));
    if (!Array.isArray(rows) || rows.length > 100000) throw new Error('Invalid storage admission ledger');
    for (const [key,value] of rows) { this.safePath(key); if (!value || typeof value.owner !== 'string' || !Number.isSafeInteger(value.bytes) || value.bytes < 0 || !Number.isSafeInteger(value.expiresAt)) throw new Error('Invalid storage charge'); this.records.set(key,value); }
   } catch(error:any) { if (error.code !== 'ENOENT') throw error; }
  })();
  await this.ready;
 }
 private safePath(relative: string) {
  const target = path.resolve(this.root, relative), inside = path.relative(this.root, target);
  if (!inside || inside.startsWith('..') || path.isAbsolute(inside)) throw new Error('Storage accounting path escapes its root');
  return target;
 }
 private async checkParents(relative: string) {
  let current = this.root;
  for (const segment of path.relative(this.root,this.safePath(relative)).split(path.sep)) {
   current = path.join(current,segment);
   try { if ((await lstat(current)).isSymbolicLink()) throw new Error('Symlinks are forbidden in admitted storage'); }
   catch(error:any) { if(error.code==='ENOENT') return; throw error; }
  }
 }
 private async bytes(target: string, counter = {files:0}): Promise<number> {
  let info; try { info=await lstat(target); } catch(error:any) { if(error.code==='ENOENT') return 0; throw error; }
  if (++counter.files > 200000) throw new AdmissionError('Storage file-count budget reached',507);
  if(info.isSymbolicLink()) throw new Error('Symlinks are forbidden in admitted storage');
  if(!info.isDirectory()) return info.size;
  let sum=0; for(const item of await readdir(target)) sum+=await this.bytes(path.join(target,item),counter);
  return sum;
 }
 private async save() {
  const temporary=path.join(this.root,`admission-ledger.${randomUUID()}.tmp`);
  try { await writeFile(temporary,JSON.stringify([...this.records])); await rename(temporary,path.join(this.root,'admission-ledger.json')); }
  finally { await rm(temporary,{force:true}); }
 }
 private serialized<T>(operation:()=>Promise<T>):Promise<T> {
  const result=this.queue.catch(()=>{}).then(operation); this.queue=result.catch(()=>{}); return result;
 }
 async seed(owner:string, relative:string, expiresAt:number, pinned=false, persist=true) {
  return this.serialized(async()=>{await this.load(); if(!this.records.has(relative)) {const bytes=await this.bytes(this.safePath(relative));if(!bytes)return;await this.checkParents(relative);this.records.set(relative,{owner,bytes,expiresAt,pinned});if(persist)await this.save();}});
 }
 retention(relative:string) { const row=this.records.get(relative);return row?{expiresAt:row.expiresAt,pinned:row.pinned}:undefined; }
 enter(req: IncomingMessage) {
  const minute=Math.floor(Date.now()/60000);
  for(const [key,value] of this.rates) if(value.minute!==minute)this.rates.delete(key);
  const address=this.trustProxy ? String(req.headers['cf-connecting-ip'] || req.socket.remoteAddress || 'unknown') : req.socket.remoteAddress || 'unknown';
  for(const [key,limit] of [['all',this.policy.globalRequestsPerMinute],[`ip:${address}`,this.policy.requestsPerMinute],[`net:${admissionNetworkKey(address)}`,this.policy.networkRequestsPerMinute]] as const) {
   const row=this.rates.get(key) || {minute,count:0}; if(row.count>=limit || this.rates.size>10000)throw new AdmissionError('Upload admission rate limit reached');row.count++;this.rates.set(key,row);
  }
  if(this.active>=this.policy.maxConcurrent)throw new AdmissionError('Upload concurrency limit reached');
  this.active++;let released=false;return()=>{if(!released){released=true;this.active--;}};
 }
 admitPublisher(owner:string) {
  const minute=Math.floor(Date.now()/60000),key=`publisher:${owner}`;
  for(const [name,value] of this.rates)if(value.minute!==minute)this.rates.delete(name);
  const row=this.rates.get(key)||{minute,count:0};
  if(row.count>=this.policy.networkRequestsPerMinute||this.rates.size>=10000)throw new AdmissionError('Publisher upload rate limit reached');
  row.count++;this.rates.set(key,row);
 }
 async cleanup(now=Date.now()) {
  return this.serialized(async()=>{await this.load();for(const [relative,row] of this.records) {await this.checkParents(relative);if(!row.pinned && row.expiresAt<=now) {await rm(this.safePath(relative),{recursive:true,force:true});this.records.delete(relative);}else {row.bytes=await this.bytes(this.safePath(relative));if(!row.bytes)this.records.delete(relative);}}await this.save();});
 }
 async renew(owner:string,relative:string,expiresAt:number) {
  return this.serialized(async()=>{await this.load();const record=this.records.get(relative);if(record&&record.owner===owner){record.expiresAt=expiresAt;await this.save();}});
 }
 async run<T>(owner:string,relative:string,reservation:number,expiresAt:number,pinned:boolean,operation:()=>Promise<T>):Promise<T> {
  if(!Number.isSafeInteger(reservation)||reservation<0)throw new Error('Invalid storage reservation');
  return this.serialized(async()=>{
   await this.load();await this.checkParents(relative);
   const previous=this.records.get(relative);
   if(!previous && this.records.size>=100000)throw new AdmissionError('Storage accounting entry budget reached',507);
   if(previous && previous.owner!==owner)throw new AdmissionError('Storage path belongs to another publisher',403);
   const owned=[...this.records.values()].filter(row=>row.owner===owner);
   const ownerBytes=owned.reduce((sum,row)=>sum+row.bytes,0), total=await this.bytes(this.root);
   const pinnedBytes=[...this.records.values()].filter(row=>row.pinned).reduce((sum,row)=>sum+row.bytes,0);
   const ownerPins=owned.filter(row=>row.pinned).reduce((sum,row)=>sum+row.bytes,0);
   const disk=await statfs(this.root);const free=Number(disk.bavail)*Number(disk.bsize);
   if(total+reservation>this.policy.totalBytes || ownerBytes+reservation>this.policy.publisherBytes || free-reservation<this.policy.minimumFreeBytes)throw new AdmissionError('Storage quota or disk watermark reached',507);
   if((pinned||previous?.pinned)&&(pinnedBytes+reservation>this.policy.pinBytes||ownerPins+reservation>this.policy.publisherPinBytes))throw new AdmissionError('IPFS pinning budget reached',507);
   let completed=false;
   try { const result=await operation();completed=true;return result; }
   finally {
    const bytes=await this.bytes(this.safePath(relative));
    if(bytes) this.records.set(relative,{owner,bytes,expiresAt:completed?expiresAt:previous?.expiresAt??Date.now()+86400000,pinned:previous?.pinned || pinned});
    else this.records.delete(relative);
    await this.save(); // Failed partial writes are charged too; retrying cannot evade the budget.
   }
  });
 }
}
