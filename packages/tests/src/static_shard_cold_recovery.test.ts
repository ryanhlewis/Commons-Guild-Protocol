import {test,expect,vi} from 'vitest';
import fs from 'node:fs/promises';import os from 'node:os';import path from 'node:path';
import {generatePrivateKey,getPublicKey,sign} from '@cgp/core';
import {RelayServer} from '@cgp/relay/src/server';import {MemoryStore} from '@cgp/relay/src/store';
import {createStaticShardSeedPlugin,STATIC_SHARD_PUBLISHER_PROTOCOL} from '@cgp/relay/src/plugins';
const slow=vi.hoisted(()=>({enabled:false,calls:0}));
vi.mock('@cgp/core',async original=>{const actual=await original<any>();return {...actual,verify:(...args:any[])=>{if(slow.enabled){slow.calls++;const until=Date.now()+10;while(Date.now()<until){/* emulate a costly actual cryptographic verification */}}return actual.verify(...args);}};});
async function fixture(mutated=false){const root=await fs.mkdtemp(path.join(os.tmpdir(),'cgp-cold-proofs-')),key=generatePrivateKey(),payloadHash='ab'.repeat(32),signature=await sign(key,payloadHash);const releases=Array.from({length:200},(_,i)=>({id:'cold-'+i,version:'one',title:'Cold '+i,storedAt:Date.now(),type:'library',releaseSha256:'cd'.repeat(32),shards:[],manifests:[],publisher:{protocol:STATIC_SHARD_PUBLISHER_PROTOCOL,publicKey:getPublicKey(key),payloadHash,signature:mutated&&i===199?'00'.repeat(64):signature}}));await fs.writeFile(path.join(root,'index.json'),JSON.stringify({releases}));return root;}
test('large cold valid publisher replay yields for real health and root discovery without skipping verification',async()=>{
 const root=await fixture();slow.enabled=true;slow.calls=0;
 const relay=new RelayServer(0,new MemoryStore(),[createStaticShardSeedPlugin({storeDir:root,autoIngest:false,storageAdmission:{minimumFreeBytes:0}}),{name:'cold-discovery',onHttp:async({pathname,res})=>{if(pathname!=='/discover')return false;res.end('ready');return true;}}],{enableDefaultPlugins:false});
 try{while(!relay.getPort()||slow.calls===0)await new Promise(resolve=>setTimeout(resolve,5));const origin='http://127.0.0.1:'+relay.getPort();
  expect((await fetch(origin+'/healthz',{signal:AbortSignal.timeout(500)})).status).toBe(200);
  expect((await fetch(origin+'/discover',{signal:AbortSignal.timeout(500)})).status).toBe(200);expect(slow.calls).toBeLessThan(200);
  const result=await(await fetch(origin+'/plugins/cgp.static-shards/catalog?id=cold-199')).json() as any;expect(result.releases[0].id).toBe('cold-199');expect(slow.calls).toBe(200);
 }finally{slow.enabled=false;await relay.close();await fs.rm(root,{recursive:true,force:true});}
},15000);
test('yielded cold recovery rejects a mutated publisher signature before exposing the requested release',async()=>{
 const root=await fixture(true);slow.enabled=true;slow.calls=0;const relay=new RelayServer(0,new MemoryStore(),[createStaticShardSeedPlugin({storeDir:root,autoIngest:false,storageAdmission:{minimumFreeBytes:0}})],{enableDefaultPlugins:false});
 try{while(!relay.getPort())await new Promise(resolve=>setTimeout(resolve,5));const response=await fetch('http://127.0.0.1:'+relay.getPort()+'/plugins/cgp.static-shards/catalog?id=cold-199');expect(response.status).toBe(500);expect(await response.text()).not.toContain('"id":"cold-199"');expect(slow.calls).toBe(200);
 }finally{slow.enabled=false;await relay.close();await fs.rm(root,{recursive:true,force:true});}
},15000);
