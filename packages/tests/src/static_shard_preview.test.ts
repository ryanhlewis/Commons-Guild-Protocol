import {createHash} from 'node:crypto';
import {createServer} from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import {afterEach,expect,it} from 'vitest';
import {generatePrivateKey,getPublicKey,hashObject,sign} from '@cgp/core';
import {createStaticShardSeedPlugin,staticShardReleaseSigningPayload,STATIC_SHARD_PUBLISHER_PROTOCOL} from '@cgp/relay/src/plugins';
import {RelayServer} from '@cgp/relay/src/server';
import {MemoryStore} from '@cgp/relay/src/store';
import {zipSync} from 'fflate';
const hash=(b:Buffer)=>createHash('sha256').update(b).digest('hex');
const relays:RelayServer[]=[],directories:string[]=[];
afterEach(async()=>{await Promise.all(relays.splice(0).map(r=>r.close()));await Promise.all(directories.splice(0).map(d=>fs.rm(d,{recursive:true,force:true})));});
async function signed(value:Record<string,unknown>,key:Uint8Array){const claim={...value,publisher:{protocol:STATIC_SHARD_PUBLISHER_PROTOCOL,publicKey:getPublicKey(key)}};return {...claim,publisher:{...claim.publisher,signature:await sign(key,hashObject(staticShardReleaseSigningPayload(claim)))}};}
async function relay(storeDir:string,extra={}){const r=new RelayServer(0,new MemoryStore(),[createStaticShardSeedPlugin({storeDir,autoIngest:false,uploadWorkBits:0,storageAdmission:{minimumFreeBytes:0},...extra})],{enableDefaultPlugins:false});relays.push(r);for(let i=0;i<200;i++){if(r.getPort()>0)return {r,base:'http://127.0.0.1:'+r.getPort()};await new Promise(resolve=>setTimeout(resolve,25));}throw Error('Relay startup timed out');}
async function fixture(id:string,key:Uint8Array){const previewUrl='https://hollow.to/home-assets/game-previews/'+'a'.repeat(64)+'.mp4?v=2';const manifest=Buffer.from(JSON.stringify({kind:'hollow-game',id,title:id,entry:{type:'web',localPath:'index.html'}}));const shard=Buffer.from(zipSync({'index.html':new TextEncoder().encode('<h1>Game</h1>')}));const release=await signed({kind:'cgp-static-shard-release',schemaVersion:1,id,title:id,type:'game',version:'1',previewUrl,display:{crossOriginIsolated:true},manifests:{game:{path:'manifest.json',sha256:hash(manifest)}},shards:[{id:'code',kind:'code',path:'code.zip',bytes:shard.length,sha256:hash(shard)}]},key);const bytes=Buffer.from(JSON.stringify(release));return {release,bytes,manifest,shard,previewUrl,upload:{releaseBase64:bytes.toString('base64'),releaseSha256:hash(bytes),files:[{path:'manifest.json',sha256:hash(manifest),bytesBase64:manifest.toString('base64')},{path:'code.zip',sha256:hash(shard),bytesBase64:shard.toString('base64')}]}};}
async function legacyRegistry(storeDir:string){const file=path.join(storeDir,'index.json'),journalFile=file+'.ndjson';const journal=(await fs.readFile(journalFile,'utf8')).trim().split('\n').map(line=>JSON.parse(line));const registry={releases:[...new Map(journal.map(row=>[row.id+'@'+row.version,row])).values()]};return {file,journalFile,registry};}
it('preserves uploaded signed previews and repairs only missing verified metadata on restore',async()=>{
 const storeDir=await fs.mkdtemp(path.join(os.tmpdir(),'cgp-preview-'));directories.push(storeDir);const key=generatePrivateKey();const first=await relay(storeDir);const fixtures=[];
 for(const id of ['preview-repair','preview-override','preview-clear','preview-tampered']){const f=await fixture(id,key);fixtures.push(f);const response=await fetch(first.base+'/plugins/cgp.static-shards/upload',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(f.upload)});const result=await response.json() as any;expect(response.status,JSON.stringify(result)).toBe(201);expect(result.release.previewUrl).toBe(f.previewUrl);}
 const override='https://media.example/new-preview.mp4?v=3';
 for(const [index,previewUrl] of [[1,override],[2,'']] as const){const f=fixtures[index];const claim=await signed({kind:'cgp-game-listing-update/1',id:f.release.id,version:'1',releaseSha256:hash(f.bytes),expectedRevision:0,title:f.release.id,previewUrl,timestamp:Date.now()},key);const response=await fetch(first.base+'/plugins/cgp.static-shards/listing',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(claim)});expect(response.status).toBe(200);}
 await first.r.close();const indexFile=path.join(storeDir,'index.json'),journalFile=indexFile+'.ndjson';const journal=(await fs.readFile(journalFile,'utf8')).trim().split('\n').map(line=>JSON.parse(line));const registry={releases:[...new Map(journal.map(row=>[row.id+'@'+row.version,row])).values()]};for(const row of registry.releases)if(row.id==='preview-repair'||row.id==='preview-tampered')delete row.previewUrl;await fs.writeFile(indexFile,JSON.stringify(registry));await fs.writeFile(journalFile,'');
 // A stale registry hash must never authorize metadata from changed raw bytes.
 const tampered=path.join(storeDir,'games/preview-tampered/1/release.json');const raw=JSON.parse(await fs.readFile(tampered,'utf8'));raw.previewUrl='https://attacker.example/clip.mp4';await fs.writeFile(tampered,JSON.stringify(raw));
 const second=await relay(storeDir);const catalog=await(await fetch(second.base+'/plugins/cgp.static-shards/catalog?limit=16')).json() as any;const rows=new Map(catalog.releases.map((r:any)=>[r.id,r]));expect((rows.get('preview-repair') as any).previewUrl).toBe(fixtures[0].previewUrl);expect((rows.get('preview-override') as any).previewUrl).toBe(override);expect((rows.get('preview-clear') as any).previewUrl).toBeUndefined();expect((rows.get('preview-tampered') as any).previewUrl).toBeUndefined();expect((rows.get('preview-repair') as any).releaseSha256).toBe(hash(fixtures[0].bytes));expect(await fs.readFile(path.join(storeDir,'games/preview-repair/1/release.json'))).toEqual(fixtures[0].bytes);
},20000);
it('preserves seeded previews but rejects changed signed seed bytes on restore even with a matching registry hash',async()=>{
 const storeDir=await fs.mkdtemp(path.join(os.tmpdir(),'cgp-preview-seed-'));directories.push(storeDir);
 const f=await fixture('preview-seed',generatePrivateKey());
 const server=createServer((req,res)=>{const b=req.url==='/release.json'?f.bytes:req.url==='/manifest.json'?f.manifest:req.url==='/code.zip'?f.shard:undefined;if(!b){res.writeHead(404).end();return;}res.end(b);});
 await new Promise<void>(resolve=>server.listen(0,'127.0.0.1',resolve));
 try {
   const port=(server.address() as any).port;
   const running=await relay(storeDir,{sources:[{kind:'release',url:'http://127.0.0.1:'+port+'/release.json'}],autoIngest:true});
   let seeded=false;
   for(let i=0;i<200;i++){const j=await(await fetch(running.base+'/plugins/cgp.static-shards/catalog?id=preview-seed')).json() as any;if(j.releases.length){expect(j.releases[0].previewUrl).toBe(f.previewUrl);seeded=true;break;}await new Promise(r=>setTimeout(r,25));}
   expect(seeded).toBe(true);await running.r.close();
   const {file,journalFile,registry}=await legacyRegistry(storeDir);
   const rawFile=path.join(storeDir,'games/preview-seed/1/release.json');
   const raw=JSON.parse(await fs.readFile(rawFile,'utf8'));raw.previewUrl='https://attacker.example/unsigned.mp4';
   const altered=Buffer.from(JSON.stringify(raw));await fs.writeFile(rawFile,altered);
   delete registry.releases[0].previewUrl;registry.releases[0].releaseSha256=hash(altered);
   await fs.writeFile(file,JSON.stringify(registry));await fs.writeFile(journalFile,'');
   const restarted=await relay(storeDir);
   const restored=await(await fetch(restarted.base+'/plugins/cgp.static-shards/catalog?id=preview-seed')).json() as any;
   expect(restored.releases[0].previewUrl).toBeUndefined();
 } finally {server.closeAllConnections();await new Promise<void>(resolve=>server.close(()=>resolve()));}
},20000);
