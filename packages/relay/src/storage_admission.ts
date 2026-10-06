import {traceAsyncPhase} from './async_phase_trace.js';
import { mkdir, readdir, lstat, readFile, writeFile, rename, rm, statfs } from 'node:fs/promises';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import type { IncomingMessage } from 'node:http';
import { admissionNetworkKey } from '@cgp/core';
import { auditWindowsStorage } from './native_windows_storage_audit.js';

export interface StorageAdmissionPolicy {
 totalBytes: number; publisherBytes: number; pinBytes: number; publisherPinBytes: number;
 minimumFreeBytes: number; maxConcurrent: number; requestsPerMinute: number; networkRequestsPerMinute: number; globalRequestsPerMinute: number;
 /** Maximum files and directories visited by one complete storage audit. */
 maxScanEntries?: number;
}
interface Charge { owner: string; bytes: number; expiresAt: number; pinned: boolean }
export interface StorageReservation { relative:string; reservation:number; expiresAt:number; pinned:boolean }
export class AdmissionError extends Error { constructor(message: string, public status = 429) { super(message); } }

/** Operator-local accounting. Published identity/log ownership is never erased by retention. */
export class StorageAdmission {
 private queue: Promise<unknown> = Promise.resolve();
 private records = new Map<string, Charge>();
 private ready: Promise<void> | null = null;
 private active = 0;
 private scanActive = 0;
 private scanWaiters: Array<()=>void> = [];
 private rates = new Map<string, { minute: number; count: number }>();
 private lastCleanupAudit?: {entries:number; bytes:number; records:number; durationMs:number};
 cleanupAuditSummary(){return this.lastCleanupAudit?{...this.lastCleanupAudit}:undefined;}
 constructor(private root: string, readonly policy: StorageAdmissionPolicy, private trustProxy = false) {
  this.root = path.resolve(root);
  const maxScanEntries = policy.maxScanEntries ?? 200000;
  if(!Number.isSafeInteger(maxScanEntries)||maxScanEntries<1||maxScanEntries>2000000)throw new Error('Invalid storage audit entry budget');
  this.policy = {...policy,maxScanEntries};
  if(Object.values(this.policy).some(value=>!Number.isSafeInteger(value)||value<0)||policy.maxConcurrent<1||policy.requestsPerMinute<1||policy.networkRequestsPerMinute<1||policy.globalRequestsPerMinute<1)throw new Error('Invalid storage admission policy');
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
 private async scanIo<T>(operation:()=>Promise<T>):Promise<T> {
  if(this.scanActive>=32)await new Promise<void>(resolve=>this.scanWaiters.push(resolve));
  else this.scanActive++;
  try{return await operation();}
  finally{const next=this.scanWaiters.shift();if(next)next();else this.scanActive--;}
 }
 private async bytes(target: string, counter = {files:0}, charges?:Map<string,number>): Promise<number> {
  if(target===this.root&&counter.files===0&&process.env.CGP_STORAGE_AUDIT_WINDOWS_NATIVE==='1'){
   const executable=process.env.CGP_STORAGE_AUDIT_POWERSHELL;
   if(!executable)throw new Error('Native storage audit executable is not configured');
   const result=await auditWindowsStorage(this.root,this.policy.maxScanEntries!,[...(charges?.keys()??[])],executable);
   counter.files=result.entries;if(charges)for(const [key,value]of result.charges)charges.set(key,value);
   return result.bytes;
  }
  let info; try { info=await this.scanIo(()=>lstat(target)); } catch(error:any) { if(error.code==='ENOENT') return 0; throw error; }
  if (++counter.files > this.policy.maxScanEntries!) throw new AdmissionError('Storage file-count budget reached',507);
  if(info.isSymbolicLink()) throw new Error('Symlinks are forbidden in admitted storage');
  if(!info.isDirectory()) {if(charges?.has(this.pathKey(target)))charges.set(this.pathKey(target),info.size);return info.size;}
  const items=await this.scanIo(()=>readdir(target));let sum=0;
  // Audit every file, including untracked bytes, without serial filesystem latency.
  // Batch recursion and cap the actual I/O across all nested directories.
  for(let offset=0;offset<items.length;offset+=32){
   const sizes=await Promise.all(items.slice(offset,offset+32).map(item=>this.bytes(path.join(target,item),counter,charges)));
   sum+=sizes.reduce((total,size)=>total+size,0);
  }
  if(charges?.has(this.pathKey(target)))charges.set(this.pathKey(target),sum);
  return sum;
 }
 private pathKey(target:string){return process.platform==='win32'?target.toLowerCase():target;}
 private async save() {
  const temporary=path.join(this.root,`admission-ledger.${randomUUID()}.tmp`);
  try { await writeFile(temporary,JSON.stringify([...this.records])); await rename(temporary,path.join(this.root,'admission-ledger.json')); }
  finally { await rm(temporary,{force:true}); }
 }
 private trace<T>(phase:string,work:()=>Promise<T>):Promise<T>{return traceAsyncPhase(phase,work,value=>{if(value.state==='complete'&&value.durationMs<1000)return;console.info('[storage-admission] at='+new Date().toISOString()+' phase='+value.phase+' state='+value.state+' durationMs='+value.durationMs);});}
 private serialized<T>(operation:()=>Promise<T>):Promise<T> {
  const previous=this.queue; const result=(async()=>{await this.trace('queue',()=>previous.catch(()=>{}));return operation();})(); this.queue=result.catch(()=>{}); return result;
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
   const row=this.rates.get(key) || {minute,count:0}; if(row.count>=limit || this.rates.size>10000){console.info('[storage-admission] rate-reject '+JSON.stringify({bucket:key==='all'?'global':key.startsWith('ip:')?'address':'network',minute,count:row.count,limit,trackedKeys:this.rates.size,retryAfterSeconds:Math.max(1,60-Math.floor(Date.now()/1000)%60)}));throw new AdmissionError('Upload admission rate limit reached');}row.count++;this.rates.set(key,row);
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
  return this.serialized(async()=>{
   await this.load();const started=Date.now(),counter={files:0},charges=new Map<string,number>();
   for(const relative of this.records.keys())charges.set(this.pathKey(this.safePath(relative)),0);
   // One fresh traversal accounts for tracked subtrees AND every untracked
   // file. Nested ledger paths each receive their exact subtree total; no
   // persisted or cached byte value is trusted as a substitute for auditing.
   const total=await this.bytes(this.root,counter,charges),removed:string[]=[];
   const expired=[...this.records].filter(([,row])=>!row.pinned&&row.expiresAt<=now).sort(([a],[b])=>a.length-b.length);
   for(const [relative] of expired){
    const target=this.safePath(relative),key=this.pathKey(target);
    if(removed.some(prefix=>key===prefix||key.startsWith(prefix+path.sep)))continue;
    await this.checkParents(relative);await rm(target,{recursive:true,force:true});removed.push(key);
   }
   for(const [relative,row] of this.records){
    const target=this.pathKey(this.safePath(relative));
    if((!row.pinned&&row.expiresAt<=now)||removed.some(prefix=>target===prefix||target.startsWith(prefix+path.sep)))this.records.delete(relative);
    else {row.bytes=charges.get(target)??0;for(const prefix of removed)if(prefix.startsWith(target+path.sep))row.bytes-=charges.get(prefix)??0;if(!row.bytes)this.records.delete(relative);}
   }
   await this.save();this.lastCleanupAudit={entries:counter.files,bytes:total,records:charges.size,durationMs:Date.now()-started};
  });
 }
 /** Reconcile confirmed absent disposable files outside another admission operation. */
 async forgetMissingFiles(relatives:string[]) {
  return this.serialized(async()=>{await this.load();for(const relative of relatives){await this.checkParents(relative);try{await lstat(this.safePath(relative));}catch(error:any){if(error.code==='ENOENT')this.records.delete(relative);else throw error;}}await this.save();});
 }
 async renew(owner:string,relative:string,expiresAt:number) {
  return this.serialized(async()=>{await this.load();const record=this.records.get(relative);if(record&&record.owner===owner){record.expiresAt=expiresAt;await this.save();}});
 }
 async run<T>(owner:string,relative:string,reservation:number,expiresAt:number,pinned:boolean,operation:()=>Promise<T>):Promise<T> {
  return this.runBatch(owner,[{relative,reservation,expiresAt,pinned}],operation);
 }
 /** Reserve a bounded group under one complete audit, never a cached root total. */
 async runBatch<T>(owner:string,entries:StorageReservation[],operation:()=>Promise<T>):Promise<T> {
  if(!entries.length||entries.length>64)throw new Error('Storage reservation batch must contain 1 to 64 paths');
  let reservation=0;const targets:string[]=[];
  for(const entry of entries){
   if(!Number.isSafeInteger(entry.reservation)||entry.reservation<0||!Number.isSafeInteger(entry.expiresAt)||typeof entry.pinned!=='boolean')throw new Error('Invalid storage reservation');
   reservation+=entry.reservation;if(!Number.isSafeInteger(reservation))throw new Error('Invalid storage reservation total');
   const resolved=this.safePath(entry.relative),target=process.platform==='win32'?resolved.toLowerCase():resolved;
   if(targets.some(existing=>existing===target||existing.startsWith(target+path.sep)||target.startsWith(existing+path.sep)))throw new Error('Storage reservation batch paths overlap');
   targets.push(target);
  }
  return this.serialized(async()=>{
   await this.load();const prior=new Map<string,Charge|undefined>();let newEntries=0,pinReservation=0,hasPinnedReservation=false;
   for(const entry of entries){
    await this.checkParents(entry.relative);const previous=this.records.get(entry.relative);prior.set(entry.relative,previous);
    if(previous&&previous.owner!==owner)throw new AdmissionError('Storage path belongs to another publisher',403);
    if(!previous)newEntries++;
    if(entry.pinned||previous?.pinned){hasPinnedReservation=true;pinReservation+=entry.reservation;}
   }
   if(this.records.size+newEntries>100000)throw new AdmissionError('Storage accounting entry budget reached',507);
   const owned=[...this.records.values()].filter(row=>row.owner===owner);
   const ownerBytes=owned.reduce((sum,row)=>sum+row.bytes,0), total=await this.trace('root-audit',()=>this.bytes(this.root));
   const pinnedBytes=[...this.records.values()].filter(row=>row.pinned).reduce((sum,row)=>sum+row.bytes,0);
   const ownerPins=owned.filter(row=>row.pinned).reduce((sum,row)=>sum+row.bytes,0);
   const disk=await this.trace('statfs',()=>statfs(this.root));const free=Number(disk.bavail)*Number(disk.bsize);
   if(total+reservation>this.policy.totalBytes || ownerBytes+reservation>this.policy.publisherBytes || free-reservation<this.policy.minimumFreeBytes)throw new AdmissionError('Storage quota or disk watermark reached',507);
   if(hasPinnedReservation&&(pinnedBytes+pinReservation>this.policy.pinBytes||ownerPins+pinReservation>this.policy.publisherPinBytes))throw new AdmissionError('IPFS pinning budget reached',507);
   let completed=false;
   try { const result=await this.trace('write-operation',operation);completed=true;return result; }
   finally {
    let accountingError:unknown;
    for(const entry of entries){
     const previous=prior.get(entry.relative);let bytes:number;
     try{bytes=await this.bytes(this.safePath(entry.relative));}
     catch(error){accountingError??=error;bytes=(previous?.bytes??0)+entry.reservation;}
     if(bytes)this.records.set(entry.relative,{owner,bytes,expiresAt:completed?entry.expiresAt:previous?.expiresAt??Date.now()+86400000,pinned:previous?.pinned||entry.pinned});
     else this.records.delete(entry.relative);
    }
    await this.trace('ledger-save',()=>this.save()); // Failed partial writes are charged too; retrying cannot evade the budget.
    if(accountingError)throw accountingError;
   }
  });
 }
}
