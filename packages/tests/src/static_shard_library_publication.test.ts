import {it,expect} from 'vitest';
import {createHash} from 'node:crypto';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {zipSync} from 'fflate';
import {generatePrivateKey,getPublicKey,hashObject,sign} from '@cgp/core';
import {createStaticShardSeedPlugin,staticShardReleaseSigningPayload,STATIC_SHARD_PUBLISHER_PROTOCOL} from '@cgp/relay/src/plugins';
import {RelayServer} from '@cgp/relay/src/server';
import {MemoryStore} from '@cgp/relay/src/store';
const hash=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
async function base(relay:RelayServer){for(let i=0;i<100;i++){if(relay.getPort())return 'http://127.0.0.1:'+relay.getPort();await new Promise(resolve=>setTimeout(resolve,20));}throw Error('Fixture relay did not listen');}

it('publishes a signed library while ctx community writes are blocked; durable catalog survives and repair runs on restart',async()=>{
 const storeDir=await fs.mkdtemp(path.join(os.tmpdir(),'cgp-library-publish-')),key=generatePrivateKey();
 const manifest=Buffer.from(JSON.stringify({kind:'hollow-game',schemaVersion:1,id:'test-artwork',title:'Source art',version:'one',entry:{type:'web',localPath:'thumbnail.webp'}})),image=Buffer.from('original-artwork-bytes'),shard=Buffer.from(zipSync({'thumbnail.webp':image}));
 const claim={kind:'cgp-static-shard-release',schemaVersion:1,id:'test-artwork',title:'Source art',type:'library',version:'one',manifests:{game:{path:'manifest.json',sha256:hash(manifest)}},shards:[{id:'art',kind:'code',path:'art.zip',bytes:shard.length,sha256:hash(shard)}],publisher:{protocol:STATIC_SHARD_PUBLISHER_PROTOCOL,publicKey:getPublicKey(key)}};
 const release={...claim,publisher:{...claim.publisher,signature:await sign(key,hashObject(staticShardReleaseSigningPayload(claim)))}};
 const bytes=Buffer.from(JSON.stringify(release)),body={releaseBase64:bytes.toString('base64'),releaseSha256:hash(bytes),files:[{path:'manifest.json',bytesBase64:manifest.toString('base64'),sha256:hash(manifest)},{path:'art.zip',bytesBase64:shard.toString('base64'),sha256:hash(shard)}]};
 let unblock!:()=>void,started!:()=>void;const blocked=new Promise<void>(resolve=>{unblock=resolve;}),communityStarted=new Promise<void>(resolve=>{started=resolve;});
 const plugin=createStaticShardSeedPlugin({storeDir,autoIngest:false,uploadWorkBits:0,storageAdmission:{minimumFreeBytes:0},registryCompactEvery:1});
 const init=plugin.onInit!;plugin.onInit=ctx=>init({...ctx,publishAsRelay:async()=>{started();await blocked;throw Error('fixture community failure');}});
 const relay=new RelayServer(0,new MemoryStore(),[plugin],{enableDefaultPlugins:false});let restarted:RelayServer|undefined;
 try{
  const origin=await base(relay),response=await fetch(origin+'/plugins/cgp.static-shards/upload',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body),signal:AbortSignal.timeout(3000)});expect(response.status).toBe(201);await communityStarted;
  const catalog=await(await fetch(origin+'/plugins/cgp.static-shards/catalog?id=test-artwork')).json() as any;expect(catalog.releases[0].releaseSha256).toBe(hash(bytes));
  const art=Buffer.from(await(await fetch(origin+'/plugins/cgp.static-shards/play/test-artwork/one/thumbnail.webp')).arrayBuffer());expect(art).toEqual(image);
  const registry=JSON.parse(await fs.readFile(path.join(storeDir,'index.json'),'utf8'));expect(registry.releases.some((r:any)=>r.id==='test-artwork')).toBe(true);
  unblock();await new Promise(resolve=>setTimeout(resolve,30));const status=await(await fetch(origin+'/plugins/cgp.static-shards/status')).json() as any;expect(JSON.stringify(status.errors)).toContain('fixture community failure');await relay.close();
  restarted=new RelayServer(0,new MemoryStore(),[createStaticShardSeedPlugin({storeDir,autoIngest:false,uploadWorkBits:0,storageAdmission:{minimumFreeBytes:0}})],{enableDefaultPlugins:false});const next=await base(restarted);
  const restored=await(await fetch(next+'/plugins/cgp.static-shards/catalog?id=test-artwork')).json() as any;expect(restored.releases[0].releaseSha256).toBe(hash(bytes));
  let repaired=false;for(let i=0;i<100;i++){const state=await(await fetch(next+'/plugins/cgp.static-shards/catalog?id=test-artwork')).json() as any;if(state.releases[0].guildId){repaired=true;break;}await new Promise(resolve=>setTimeout(resolve,20));}expect(repaired).toBe(true);
 }finally{unblock();await relay.close();await restarted?.close();await fs.rm(storeDir,{recursive:true,force:true});}
},20000);
