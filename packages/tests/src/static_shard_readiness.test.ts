import {test,expect,vi} from 'vitest';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import WebSocket from 'ws';
import {RelayServer} from '@cgp/relay/src/server';
import {MemoryStore} from '@cgp/relay/src/store';
import {createStaticShardSeedPlugin,createHollowRoomRelayPlugin} from '@cgp/relay/src/plugins';
const gate=vi.hoisted(()=>({path:'',blocked:undefined as Promise<void>|undefined}));
vi.mock('node:fs/promises',async original=>{const actual=await original<typeof import('node:fs/promises')>();return {...actual,readFile:async(...args:any[])=>{if(String(args[0])===gate.path)await gate.blocked;return (actual.readFile as any)(...args);}};});
test('blocked static registry recovery gates its reads and writes but not relay discovery, health or room JOIN',async()=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'cgp-static-ready-'));let release!:()=>void;
 gate.path=path.join(root,'index.json');gate.blocked=new Promise<void>(resolve=>{release=resolve;});
 const plugin=createStaticShardSeedPlugin({storeDir:root,autoIngest:false,uploadWorkBits:0,storageAdmission:{minimumFreeBytes:0}}),relay=new RelayServer(0,new MemoryStore(),[plugin,createHollowRoomRelayPlugin(),{name:'ready-discovery',onHttp:async({pathname,res})=>{if(pathname!=='/discover')return false;res.statusCode=200;res.end('ready');return true;}}],{enableDefaultPlugins:false});let socket:WebSocket|undefined;
 try{
  while(!relay.getPort())await new Promise(resolve=>setTimeout(resolve,10));const origin=`http://127.0.0.1:${relay.getPort()}`;
  const catalog=fetch(origin+'/plugins/cgp.static-shards/catalog?id=absent'),upload=fetch(origin+'/plugins/cgp.static-shards/upload-status',{method:'POST',headers:{'content-type':'application/json'},body:'{}'});let catalogSettled=false,uploadSettled=false;void catalog.then(()=>{catalogSettled=true;});void upload.then(()=>{uploadSettled=true;});
  expect((await fetch(origin+'/discover',{signal:AbortSignal.timeout(2000)})).status).toBe(200);
  expect((await fetch(origin+'/assets/runtime.wasm',{signal:AbortSignal.timeout(2000)})).status).toBe(503);
  expect((await fetch(origin+'/healthz',{signal:AbortSignal.timeout(2000)})).status).toBe(200);
  expect((await fetch(origin+'/plugins/hollow-relay/relays',{signal:AbortSignal.timeout(2000)})).status).toBe(200);
  socket=new WebSocket(origin.replace('http:','ws:'));const joined=new Promise<any>((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('JOIN blocked by static registry')),2000);socket!.on('message',raw=>{const frame=JSON.parse(String(raw));if(frame[0]==='JOINED'){clearTimeout(timer);resolve(frame[1]);}});socket!.once('open',()=>socket!.send(JSON.stringify(['JOIN',{roomId:'server:static-recovery-test'}])));});
  expect((await joined).roomId).toBe('server:static-recovery-test');expect(catalogSettled).toBe(false);expect(uploadSettled).toBe(false);
  release();expect((await catalog).status).toBe(200);expect((await upload).status).toBeGreaterThanOrEqual(400);
 }finally{release();gate.path='';socket?.terminate();await relay.close();await fs.rm(root,{recursive:true,force:true});}
},60000);
