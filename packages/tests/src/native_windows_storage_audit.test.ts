import {test,expect} from 'vitest';
import {mkdtemp,mkdir,writeFile,rm,lstat,readdir,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {auditWindowsStorage,validateNativeAudit} from '@cgp/relay/src/native_windows_storage_audit';
import {StorageAdmission} from '@cgp/relay/src/storage_admission';
const executable='C:/Users/cortanium/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/powershell/pwsh.exe';
async function independent(target:string):Promise<{entries:number;bytes:number}>{const info=await lstat(target);if(info.isSymbolicLink())throw new Error('link');let entries=1,bytes=info.isDirectory()?0:info.size;if(info.isDirectory())for(const name of await readdir(target)){const child=await independent(path.join(target,name));entries+=child.entries;bytes+=child.bytes;}return {entries,bytes};}
test.skipIf(process.platform!=='win32')('fresh native traversal matches independent tracked, untracked, overlapping and missing accounting',async()=>{
 const root=await mkdtemp(path.join(tmpdir(),'cgp-native-audit-'));
 try{
  await mkdir(path.join(root,'parent','child'),{recursive:true});await writeFile(path.join(root,'parent','a'),Buffer.alloc(17));await writeFile(path.join(root,'parent','child','b'),Buffer.alloc(31));await writeFile(path.join(root,'untracked'),Buffer.alloc(43));
  await writeFile(path.join(root,'雪 café'),Buffer.alloc(7));
  const tracked=['parent','parent/child','missing','untracked','雪 café'].map(p=>path.join(root,p));
  const result=await auditWindowsStorage(root,100,tracked,executable);expect({entries:result.entries,bytes:result.bytes}).toEqual(await independent(root));
  expect([...result.charges.values()]).toEqual([48,31,0,43,7]);
  await writeFile(path.join(root,'new-untracked'),Buffer.alloc(101));expect((await auditWindowsStorage(root,100,tracked,executable)).bytes).toBe(result.bytes+101);
  await expect(auditWindowsStorage(root,2,tracked,executable)).rejects.toThrow('failed');
  const timeout=await auditWindowsStorage(root,100,tracked,executable,1).catch(error=>error as Error);
  expect(timeout).toBeInstanceOf(Error);
  expect((timeout as Error).message).toMatch(/timed out \(phase=(spawn|compile|walk|serialize), entries=\d+, elapsedMs=\d+, outputBytes=\d+, eventLoopDelayMs=\d+\)/);
  expect((timeout as Error).message).not.toContain(root);
  await symlink(path.join(root,'parent'),path.join(root,'junction'),'junction');await expect(auditWindowsStorage(root,100,tracked,executable)).rejects.toThrow('failed');
  await expect(auditWindowsStorage(path.join(root,'junction'),100,[],executable)).rejects.toThrow('failed');
 }finally{await rm(root,{recursive:true,force:true});}
},120000);
test('native audit rejects untrusted executable configuration',async()=>{await expect(auditWindowsStorage('C:/store',100,[],'cmd.exe')).rejects.toThrow('absolute trusted');});
test('native process reports reject malformed, missing, extra, duplicate and unsafe totals',()=>{
 const key='C:/store/a',base={entries:2,bytes:8,charges:[[key,8]]};
 expect(validateNativeAudit(JSON.stringify(base),10,[key]).bytes).toBe(8);
 for(const value of [{...base,entries:11},{...base,bytes:Number.MAX_SAFE_INTEGER+1},{...base,charges:[]},{...base,charges:[[key,9]]},{...base,charges:[['C:/other',8]]},{...base,charges:[[key,4],[key,4]]}])expect(()=>validateNativeAudit(JSON.stringify(value),10,[key])).toThrow();
 expect(()=>validateNativeAudit('not JSON',10,[key])).toThrow();
});
test.skipIf(process.platform!=='win32')('native audit failure cannot execute an admitted write',async()=>{
 const root=await mkdtemp(path.join(tmpdir(),'cgp-native-admit-')),oldEnabled=process.env.CGP_STORAGE_AUDIT_WINDOWS_NATIVE,oldExecutable=process.env.CGP_STORAGE_AUDIT_POWERSHELL;
 try{
  process.env.CGP_STORAGE_AUDIT_WINDOWS_NATIVE='1';process.env.CGP_STORAGE_AUDIT_POWERSHELL=executable;
  await writeFile(path.join(root,'untracked'),Buffer.alloc(3));
  const policy={totalBytes:100000,publisherBytes:100000,pinBytes:100000,publisherPinBytes:100000,minimumFreeBytes:0,maxConcurrent:1,requestsPerMinute:10,networkRequestsPerMinute:10,globalRequestsPerMinute:10,maxScanEntries:1};
  const admission=new StorageAdmission(root,policy);let wrote=false;
  await expect(admission.run('publisher','new',1,Date.now()+10000,false,async()=>{wrote=true;})).rejects.toThrow('failed');expect(wrote).toBe(false);
 }finally{if(oldEnabled===undefined)delete process.env.CGP_STORAGE_AUDIT_WINDOWS_NATIVE;else process.env.CGP_STORAGE_AUDIT_WINDOWS_NATIVE=oldEnabled;if(oldExecutable===undefined)delete process.env.CGP_STORAGE_AUDIT_POWERSHELL;else process.env.CGP_STORAGE_AUDIT_POWERSHELL=oldExecutable;await rm(root,{recursive:true,force:true});}
},60000);
