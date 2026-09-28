import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {CgpClient} from '@cgp/client';
import {generatePrivateKey,getPublicKey,hashObject,assertRelayHeadQuorum,verifyRelayWriteCertificate} from '@cgp/core';
import {RelayServer,LocalRelayPubSubAdapter,type RelayPubSubAdapter,type RelayPubSubEnvelope,type RelayPubSubSubscribeOptions} from '@cgp/relay/src/server';
import {LevelStore} from '@cgp/relay/src/store_level';
const runId=`local-quorum-${Date.now()}`,output=path.resolve('output/community-continuity',runId);fs.mkdirSync(output,{recursive:true});
const bus=new LocalRelayPubSubAdapter();
class Link implements RelayPubSubAdapter {
 isolated=false; subscriptions=new Set<any>();
 publish(topic:string,envelope:RelayPubSubEnvelope){if(!this.isolated)bus.publish(topic,envelope);}
 subscribe(topic:string,handler:(e:RelayPubSubEnvelope)=>void,options?:RelayPubSubSubscribeOptions){const sub={topic,handler,options,remove:undefined as any};this.subscriptions.add(sub);if(!this.isolated)sub.remove=bus.subscribe(topic,handler,options);return()=>{sub.remove?.();this.subscriptions.delete(sub);};}
 setIsolated(value:boolean){this.isolated=value;for(const sub of this.subscriptions){sub.remove?.();sub.remove=undefined;if(!value)sub.remove=bus.subscribe(sub.topic,sub.handler,sub.options);}}
 isReady(){return !this.isolated;}
 close(){for(const sub of this.subscriptions)sub.remove?.();this.subscriptions.clear();}
}
const keys=Array.from({length:3},()=>generatePrivateKey()),quorum={epoch:runId,members:keys.map(getPublicKey),requiredVotes:2,voteTimeoutMs:250};
const links=keys.map(()=>new Link()),stores:LevelStore[]=[],relays:RelayServer[]=[],peers:CgpClient[]=[];const checks:string[]=[];const ownedPorts:number[]=[];
const priv=generatePrivateKey(),identity={priv,pub:getPublicKey(priv)},guild=hashObject({runId,owner:identity.pub}),channel=hashObject({guild});
const pause=(ms:number)=>new Promise(r=>setTimeout(r,ms));
async function start(index:number){stores[index]=new LevelStore(path.join(output,`store-${index}`));relays[index]=new RelayServer(0,stores[index],[],{listenHost:'127.0.0.1',enableDefaultPlugins:false,sequencerConsensus:false,relayPrivateKeyHex:Buffer.from(keys[index]).toString('hex'),pubSubAdapter:links[index],writeQuorum:quorum});while(!relays[index].getPort())await pause(10);ownedPorts.push(relays[index].getPort());peers[index]=new CgpClient({relays:[`ws://127.0.0.1:${relays[index].getPort()}`],keyPair:identity});await peers[index].connect();assert((peers[index] as any).sockets.some((s:any)=>s.readyState===1));}
async function converge(seq:number){for(let attempt=0;attempt<200;attempt++){const heads=await Promise.all(peers.map(p=>p.getRelayHead(guild).catch(()=>null)));if(heads.every(h=>h?.headSeq===seq)){assertRelayHeadQuorum(guild,heads as any,{minValidHeads:3,minCanonicalCount:3});return;}await pause(25);}throw Error(`convergence failed at ${seq}`);}
const message=(id:string)=>({type:'MESSAGE' as const,guildId:guild,channelId:channel,messageId:id,content:id});
async function main(){
 for(let i=0;i<3;i++)await start(i);
 await peers[0].publishReliable({type:'GUILD_CREATE',guildId:guild,name:'Local-only synthetic quorum'},{timeoutMs:5000});for(const peer of peers)await peer.subscribe(guild);await converge(0);
 await peers[0].publishReliable({type:'CHANNEL_CREATE',guildId:guild,channelId:channel,name:'matrix',kind:'text'},{timeoutMs:5000});await converge(1);let seq=1;
 for(let minority=0;minority<3;minority++){
  links[minority].setIsolated(true);assert(links[minority].isolated);const before=await peers[minority].getRelayHead(guild);
  await assert.rejects(peers[minority].publishReliable(message(`denied-${minority}`),{timeoutMs:3000}),/quorum unavailable|1\/2 votes/i);assert.equal((await peers[minority].getRelayHead(guild)).headHash,before.headHash);
  await peers[(minority+1)%3].publishReliable(message(`pair-${minority}`),{timeoutMs:5000});seq++;assert.equal((await peers[minority].getRelayHead(guild)).headSeq,seq-1);
  links[minority].setIsolated(false);await converge(seq);checks.push(`singleton ${minority} rejects, remaining pair commits, all three exact signed heads heal`);
 }
 links.forEach(link=>link.setIsolated(true));for(let i=0;i<3;i++){await assert.rejects(peers[i].publishReliable(message(`all-denied-${i}`),{timeoutMs:3000}),/quorum unavailable|1\/2 votes/i);assert.equal((await peers[i].getRelayHead(guild)).headSeq,seq);}checks.push('all three isolated reject writes with unchanged durable heads');links.forEach(link=>link.setIsolated(false));await converge(seq);
 const logs=await Promise.all(stores.map(store=>store.getLog(guild)));for(const log of logs){assert.deepEqual(log.map(e=>e.id),logs[0].map(e=>e.id));for(const event of log){assert(verifyRelayWriteCertificate(event));assert.equal(event.writeCertificate!.policy.epoch,quorum.epoch);assert.equal(event.writeCertificate!.policy.requiredVotes,2);assert.deepEqual(event.writeCertificate!.policy.members,quorum.members);}}
 checks.push('all three logs match and every event verifies exact fixed majority certificate');
 const before=await peers[2].getRelayHead(guild),beforePort=relays[2].getPort();peers[2].close();await relays[2].close();links[2]=new Link();await start(2);const after=await peers[2].getRelayHead(guild);assert.equal(after.headHash,before.headHash);assert.equal(after.relayPublicKey,before.relayPublicKey);checks.push('third LevelDB closes and reopens with same key and exact persisted signed head');
 const endpoints=relays.map((relay,i)=>({host:`local-${i}`,url:`ws://127.0.0.1:${relay.getPort()}`,publicKey:quorum.members[i]}));
 fs.writeFileSync(path.join(output,'signed-events.json'),JSON.stringify(logs[0],null,2));fs.writeFileSync(path.join(output,'restart.json'),JSON.stringify({before,after,beforePort,afterPort:relays[2].getPort(),scope:'store and relay object reopened in same process'},null,2));
 const receipt={ok:true,pid:process.pid,ownedPorts,runId,quorum,endpoints,checks,scope:['LOCAL ONLY: one process, one machine, three loopback relays and separate LevelDB stores','No public traffic, SSH, tunnels, remote machines or independent operators','fixed membership; sequencer consensus disabled; no live epoch transition'],control:{finishFile:path.join(output,'finish'),partitionFile:path.join(output,'partition.json'),partitionAck:path.join(output,'partition-ack.json')}};
 fs.writeFileSync(path.join(output,'client-window.json'),JSON.stringify(receipt,null,2));console.log(JSON.stringify(receipt));
 if(process.argv.includes('--client-window')){const until=Date.now()+15*60000;let previous='';while(!fs.existsSync(path.join(output,'finish'))&&Date.now()<until){
 const file=path.join(output,'partition.json');if(fs.existsSync(file)){const raw=fs.readFileSync(file,'utf8');if(raw!==previous){const value=JSON.parse(raw);assert(Array.isArray(value.isolated)&&value.isolated.every((i:unknown)=>Number.isInteger(i)&&Number(i)>=0&&Number(i)<3),'Invalid local partition indices');links.forEach((link,i)=>link.setIsolated(value.isolated.includes(i)));previous=raw;fs.writeFileSync(path.join(output,'partition-ack.json'),JSON.stringify({id:value.id,isolated:links.map((link,i)=>link.isolated?i:null).filter(i=>i!==null)}));}}
 await pause(100);}assert(fs.existsSync(path.join(output,'finish')),'local client window expired');}
}
let failure:string|undefined;main().catch(error=>{failure=error.message;}).finally(async()=>{peers.forEach(p=>p.close());await Promise.all(relays.map(r=>r.close()));bus.close();fs.writeFileSync(path.join(output,'report.json'),JSON.stringify({pid:process.pid,ownedPorts,runId,ok:!failure,failure,checks,cleaned:true,scope:'LOCAL ONLY, three in-process loopback relays, no distributed claim'},null,2));console.log(JSON.stringify({runId,ok:!failure,failure,checks,cleaned:true}));if(failure)process.exitCode=1;});
