import {expect,test} from 'vitest';
import fs from 'node:fs/promises';import os from 'node:os';import path from 'node:path';
import {hashObject} from '@cgp/core';
import {createStaticShardSeedPlugin} from '@cgp/relay/src/plugins';

async function replay(missingShard=false,changedShard=false,missingSocialChannel=false){
 const root=await fs.mkdtemp(path.join(os.tmpdir(),'cgp-replay-idempotent-'));
 const id='replay-world',guildId=hashObject({kind:'cgp-static-shard-game-guild',id});
 const channelId=hashObject({kind:'cgp-static-shard-game-release-channel',id});
 const social={communityGuildId:guildId,commentsChannelId:hashObject({kind:'cgp-static-shard-game-social-channel',id,name:'comments'})};
 const releases=['old','new'].map((version,index)=>({id,version,title:'World',type:'game',guildId,storedAt:1000+index,releaseSha256:String(index+1).repeat(64),manifests:[],shards:[{id:'world',path:'world.zip',bytes:10,sha256:'ab'.repeat(32)}]}));
 const state:any={channels:new Map([channelId,...['chat','comments','Lobby'].map(name=>hashObject({kind:'cgp-static-shard-game-social-channel',id,name}))].map(key=>[key,{}])),roles:new Map(),members:new Map(),appObjects:new Map()};
 if(missingSocialChannel)state.channels.delete(hashObject({kind:'cgp-static-shard-game-social-channel',id,name:'chat'}));
 const row=(namespace:string,objectType:string,objectId:string,value:any)=>state.appObjects.set(namespace+objectType+objectId,{namespace,objectType,objectId,value});
 for(const release of releases){row('org.cgp.games','game-release',id+'@'+release.version,{releaseSha256:release.releaseSha256,...social});
  if(!missingShard||release.version!=='old')row('org.cgp.games','static-shard',id+'@'+release.version+':world',{gameId:id,version:release.version,...release.shards[0],...(changedShard&&release.version==='old'?{sha256:'ff'.repeat(32)}:{}),releaseSha256:release.releaseSha256,mirroredAt:release.storedAt});
 }
 const latest=releases[1];row('app.hollow.home','game-profile','game:'+id,{...social,version:latest.version,releaseSha256:latest.releaseSha256,updatedAt:new Date(latest.storedAt).toISOString()});
 await fs.writeFile(path.join(root,'index.json'),JSON.stringify({releases}));
 const writes:any[]=[];let stateReads=0;const plugin=createStaticShardSeedPlugin({storeDir:root,autoIngest:false,storageAdmission:{minimumFreeBytes:0}});
 try{await plugin.onInit!({getState:async()=>{stateReads++;return state;},publishAsRelay:async(body:any)=>{writes.push(body);if(body.type==='CHANNEL_CREATE')state.channels.set(body.channelId,body);else row(body.namespace,body.objectType,body.objectId,body.value);}} as any);
  const expectedReads=missingSocialChannel?3:2;
  const deadline=Date.now()+2000;while(stateReads<expectedReads&&Date.now()<deadline)await new Promise(resolve=>setTimeout(resolve,5));expect(stateReads).toBe(expectedReads);
  await new Promise<void>(resolve=>setImmediate(resolve));await plugin.onClose!();return writes;}
 finally{await fs.rm(root,{recursive:true,force:true});}
}
test('cold historical and latest release restoration does not rewrite identical durable shard or Home rows',async()=>{
 expect(await replay()).toEqual([]);
});
test('cold historical release restoration repairs only the missing shard and preserves newer Home',async()=>{
 const writes=await replay(true);expect(writes).toHaveLength(1);
 expect(writes[0]).toMatchObject({type:'APP_OBJECT_UPSERT',objectType:'static-shard',objectId:'replay-world@old:world'});
});
test('cold restoration cannot treat a changed shard digest as already registered',async()=>{
 const writes=await replay(false,true);expect(writes).toHaveLength(1);
 expect(writes[0].value.sha256).toBe('ab'.repeat(32));
});
test('cold restoration rereads authoritative state after repairing a missing social channel',async()=>{
 const writes=await replay(false,false,true);expect(writes).toHaveLength(1);
 expect(writes[0]).toMatchObject({type:'CHANNEL_CREATE',name:'chat'});
});
