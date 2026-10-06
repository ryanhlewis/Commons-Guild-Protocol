import {it,expect} from 'vitest';
import {createServer} from 'node:http';
import {once} from 'node:events';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {StaticShardCommunityQueue,publishDurableStaticLibrary} from './static_shard_community_queue.js';

it('a blocked community projection cannot hold durable library HTTP publication or catalog reads',async()=>{
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'cgp-library-durable-'));
 const release={id:'world-artwork',version:'artwork-one',type:'library',releaseSha256:'a'.repeat(64)};
 const catalog=new Map<string,typeof release>();let unblock!:()=>void,started!:()=>void;
 const blocked=new Promise<void>(resolve=>{unblock=resolve;}),startedPromise=new Promise<void>(resolve=>{started=resolve;});
 const queue=new StaticShardCommunityQueue<typeof release>(async()=>{started();await blocked;},()=>{throw Error('Unexpected repair failure');});
 const server=createServer(async(req,res)=>{
  if(req.url==='/publish'){await publishDurableStaticLibrary(release,{persist:async value=>{await fs.writeFile(path.join(root,'release.json'),JSON.stringify(value));},remember:value=>{catalog.set(value.id,value);},enqueue:value=>queue.enqueue(value)});res.end('published');}
  else res.end(JSON.stringify(catalog.get(release.id)));
 });server.listen(0,'127.0.0.1');await once(server,'listening');
 const origin=`http://127.0.0.1:${(server.address() as {port:number}).port}`;
 try{
  expect(await (await fetch(origin+'/publish',{signal:AbortSignal.timeout(1000)})).text()).toBe('published');await startedPromise;
  expect(JSON.parse(await fs.readFile(path.join(root,'release.json'),'utf8'))).toEqual(release);
  expect(await (await fetch(origin+'/catalog',{signal:AbortSignal.timeout(1000)})).json()).toEqual(release);
 }finally{unblock();await queue.idle();queue.close();await new Promise<void>(resolve=>server.close(()=>resolve()));await fs.rm(root,{recursive:true,force:true});}
});

it('bounds active plus pending repair, deduplicates, records failures, and repairs durable entries on restart',async()=>{
 const rows=[0,1,2].map(i=>({id:'library-'+i,version:'one',type:'library'}));let unblock!:()=>void;
 const blocked=new Promise<void>(resolve=>{unblock=resolve;}),errors:string[]=[],calls:string[]=[];
 const queue=new StaticShardCommunityQueue<typeof rows[number]>(async row=>{calls.push(row.id);await blocked;throw Error('community unavailable');},(row,error)=>errors.push(row.id+':'+String(error)),2);
 for(const row of rows)queue.enqueue(row);queue.enqueue(rows[0]);
 expect(errors).toHaveLength(1);expect(errors[0]).toContain('queue full');
 await new Promise<void>(resolve=>setImmediate(resolve));expect(calls).toEqual(['library-0']);unblock();await queue.idle();expect(calls).toEqual(['library-0','library-1']);expect(errors).toHaveLength(3);queue.close();
 // Startup already replays the persisted catalog; a fresh queue retries all
 // durable entries, including capacity-overflow entries, without an error loop.
 const repaired:string[]=[],restart=new StaticShardCommunityQueue<typeof rows[number]>(async row=>{repaired.push(row.id);},()=>{throw Error('Unexpected restart failure');});
 for(const row of rows)restart.enqueue(row);await restart.idle();expect(repaired).toEqual(rows.map(row=>row.id));restart.close();
});

it('does not expose failed persistence and refuses deferred game registration',async()=>{
 let remembered=false,queued=false;const actions={persist:async()=>{throw Error('disk failure');},remember:()=>{remembered=true;},enqueue:()=>{queued=true;}};
 await expect(publishDurableStaticLibrary({type:'library'},actions)).rejects.toThrow('disk failure');expect(remembered).toBe(false);expect(queued).toBe(false);
 await expect(publishDurableStaticLibrary({type:'game'},actions)).rejects.toThrow('library-only');
});
