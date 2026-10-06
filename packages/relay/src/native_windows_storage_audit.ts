import { spawn } from 'node:child_process';
import path from 'node:path';
import {StringDecoder} from 'node:string_decoder';

export interface NativeAuditResult { entries:number; bytes:number; charges:Map<string,number> }
/** Only the repository-owned script executes; all storage paths arrive as JSON data. */
export async function auditWindowsStorage(root:string,maxScanEntries:number,tracked:string[],executable:string,timeoutMs=120000):Promise<NativeAuditResult>{
 if(process.platform!=='win32'||!path.isAbsolute(executable)||!/^pwsh\.exe$/i.test(path.basename(executable)))throw new Error('Native storage audit requires an absolute trusted pwsh.exe path');
 if(tracked.length>100000)throw new Error('Native audit tracked path limit');
 const input=JSON.stringify({root,maxScanEntries,tracked}),limit=32*1024*1024;
 if(Buffer.byteLength(input)>limit)throw new Error('Native audit input limit');
 const script=path.join(__dirname,'windows_storage_audit.ps1');
 const output=await new Promise<string>((resolve,reject)=>{
  const child=spawn(executable,['-NoLogo','-NoProfile','-NonInteractive','-File',script],{windowsHide:true,stdio:['pipe','pipe','pipe']});
  let data='',size=0,errorSize=0,settled=false,phase='spawn',entries=0,diagnostics='';const decoder=new StringDecoder('utf8');
  const started=Date.now();let lastTick=started,maxEventLoopDelayMs=0,phaseStarted=started;const observedPhasesMs:Record<string,number>={};
  const report=(state:string)=>console.info('[storage-native-audit]',JSON.stringify({at:new Date().toISOString(),state,phase,entries,elapsedMs:Date.now()-started,outputBytes:size,eventLoopDelayMs:Math.max(0,maxEventLoopDelayMs),observedPhasesMs}));
  const heartbeat=setInterval(()=>{const now=Date.now();maxEventLoopDelayMs=Math.max(maxEventLoopDelayMs,now-lastTick-250);lastTick=now;},250);
  const pending=setTimeout(()=>report('pending'),10000);
  const finish=(error?:Error)=>{if(settled)return;settled=true;clearTimeout(timer);clearTimeout(pending);clearInterval(heartbeat);observedPhasesMs[phase]=(observedPhasesMs[phase]??0)+Date.now()-phaseStarted;if(Date.now()-started>=1000||error)report(error?'failed':'complete');if(error){child.kill();reject(error);}else resolve(data);};
  const timer=setTimeout(()=>finish(new Error(`Native storage audit timed out (phase=${phase}, entries=${entries}, elapsedMs=${Date.now()-started}, outputBytes=${size}, eventLoopDelayMs=${Math.max(0,maxEventLoopDelayMs)})`)),timeoutMs);
  child.stdout.on('data',(chunk:Buffer)=>{size+=chunk.length;if(size>limit)finish(new Error('Native storage audit output limit'));else data+=decoder.write(chunk);});
  child.stderr.on('data',(chunk:Buffer)=>{errorSize+=chunk.length;if(errorSize>65536){finish(new Error('Native storage audit diagnostic limit'));return;}diagnostics+=chunk.toString('utf8');const lines=diagnostics.split(/\r?\n/);diagnostics=lines.pop()!;for(const line of lines){const match=/^CGP_AUDIT phase=(compile|walk|serialize) entries=(\d+)$/.exec(line);if(match){if(phase!==match[1]){observedPhasesMs[phase]=(observedPhasesMs[phase]??0)+Date.now()-phaseStarted;phaseStarted=Date.now();phase=match[1];}entries=Number(match[2]);}}});
  child.on('error',error=>finish(error));child.stdin.on('error',error=>finish(error));
  child.on('close',code=>{data+=decoder.end();finish(code===0?undefined:new Error('Native storage audit failed'));});child.stdin.end(input);
 });
 return validateNativeAudit(output,maxScanEntries,tracked);
}
export function validateNativeAudit(output:string,maxScanEntries:number,tracked:string[]):NativeAuditResult{
 const result=JSON.parse(output),safe=(value:unknown)=>Number.isSafeInteger(value)&&Number(value)>=0;
 if(!safe(result.entries)||result.entries<1||result.entries>maxScanEntries||!safe(result.bytes)||!Array.isArray(result.charges)||result.charges.length!==tracked.length)throw new Error('Invalid native storage audit result');
 const requested=new Set(tracked.map(item=>item.toLowerCase())),charges=new Map<string,number>();
 for(const row of result.charges){if(!Array.isArray(row)||row.length!==2||typeof row[0]!=='string'||!requested.has(row[0].toLowerCase())||charges.has(row[0].toLowerCase())||!safe(row[1])||row[1]>result.bytes)throw new Error('Invalid native storage audit charge');charges.set(row[0].toLowerCase(),row[1]);}
 return {entries:result.entries,bytes:result.bytes,charges};
}
