import {allowFixtureHostname,fixtureDnsObservations} from "./fixture-dns.js";
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {CgpClient} from '@cgp/client';
import {generatePrivateKey,getPublicKey,hashObject,assertRelayHeadQuorum,verifyRelayWriteCertificate} from '@cgp/core';
import {echoPowerShell,workerPowerShell} from '../../hollow-tauri/scripts/ops/remote-ssh.mjs';
const file=path.resolve(process.argv[2]),window=JSON.parse(fs.readFileSync(file,'utf8')),output=path.dirname(file);
const pause=(ms:number)=>new Promise(r=>setTimeout(r,ms));
const priv=generatePrivateKey(),identity={priv,pub:getPublicKey(priv)},peers:any[]=[];const checks:string[]=[];
const dirs=[path.join(output,`hollow-roadmap-staging-${window.runId}`,'relay'),`C:/Users/ECHO/hollow-roadmap-staging-${window.runId}`,`C:/Users/PC/hollow-roadmap-staging-${window.runId}`];
async function isolate(index:number,on:boolean){const target=dirs[index]+'/isolate';if(index===0){if(on)fs.writeFileSync(target,'matrix');else fs.rmSync(target,{force:true});}else{const script=on?`[IO.File]::WriteAllText('${target}','matrix')`:`if(Test-Path -LiteralPath '${target}'){Remove-Item -LiteralPath '${target}' -ErrorAction Stop}`;await(index===1?echoPowerShell(script):workerPowerShell('cortop3',script));}for(let attempt=0;attempt<60;attempt++){const filename=dirs[index]+'/network-state.json';let state:any;
 if(index===0){try{state=JSON.parse(fs.readFileSync(filename,'utf8'));}catch{}}
 else{const result=await(index===1?echoPowerShell(`if(Test-Path '${filename}'){Get-Content '${filename}' -Raw}`):workerPowerShell('cortop3',`if(Test-Path '${filename}'){Get-Content '${filename}' -Raw}`));try{state=JSON.parse(result.stdout);}catch{}}
 if(state?.isolated===on)return;await pause(250);}throw Error('isolation acknowledgement timeout');}
async function converge(seq:number){for(let i=0;i<80;i++){const heads=await Promise.all(peers.map(p=>p.getRelayHead(guild).catch(()=>null)));if(heads.every(h=>h?.headSeq===seq)){assertRelayHeadQuorum(guild,heads,{minValidHeads:3,minCanonicalCount:3});return;}await pause(500);}throw Error('head convergence timeout');}
const guild=hashObject({run:window.runId,matrix:identity.pub}),channel=hashObject({guild});let seq=1;
async function main(){
try{
 for(const endpoint of window.endpoints){allowFixtureHostname(endpoint.url);const client=new CgpClient({relays:[endpoint.url],keyPair:identity});peers.push(client);await client.connect();for(let attempt=0;attempt<100 && !(client as any).sockets.some((s:any)=>s.readyState===1);attempt++)await pause(100);assert((client as any).sockets.some((s:any)=>s.readyState===1));}
 await peers[0].publishReliable({type:'GUILD_CREATE',guildId:guild,name:'All pairs synthetic'},{timeoutMs:25000});
 for(const peer of peers)await peer.subscribe(guild);await converge(0);
 await peers[0].publishReliable({type:'CHANNEL_CREATE',guildId:guild,channelId:channel,name:'matrix',kind:'text'},{timeoutMs:25000});await converge(seq);
 for(let minority=0;minority<3;minority++){
  await isolate(minority,true);const before=await peers[minority].getRelayHead(guild);
  await assert.rejects(peers[minority].publishReliable({type:'MESSAGE',guildId:guild,channelId:channel,messageId:`denied-${minority}`,content:'no majority'},{timeoutMs:20000}),/quorum unavailable|1\/2 votes/i);
  assert.equal((await peers[minority].getRelayHead(guild)).headHash,before.headHash);
  const writer=(minority+1)%3;await peers[writer].publishReliable({type:'MESSAGE',guildId:guild,channelId:channel,messageId:`pair-${minority}`,content:'surviving pair commits'},{timeoutMs:25000});seq++;
  assert.equal((await peers[minority].getRelayHead(guild)).headSeq,seq-1);
  await isolate(minority,false);await converge(seq);checks.push(`singleton ${window.endpoints[minority].host} rejected; other pair committed; three signed heads healed`);
 }
 for(let i=0;i<3;i++)await isolate(i,true);
 for(let i=0;i<3;i++){await assert.rejects(peers[i].publishReliable({type:'MESSAGE',guildId:guild,channelId:channel,messageId:`all-denied-${i}`,content:'fully partitioned'},{timeoutMs:20000}),/quorum unavailable|1\/2 votes/i);assert.equal((await peers[i].getRelayHead(guild)).headSeq,seq);}
 checks.push('all three isolated simultaneously: every publish rejected and durable heads unchanged');
 for(let i=0;i<3;i++)await isolate(i,false);await converge(seq);
 fs.writeFileSync(path.join(dirs[0],'inspect.json'),JSON.stringify({guild}));await pause(1000);
 const snapshot=JSON.parse(fs.readFileSync(path.join(dirs[0],'snapshot.json'),'utf8'));assert.equal(snapshot.events.length,seq+1);
 for(const event of snapshot.events){assert(verifyRelayWriteCertificate(event));assert.equal(event.writeCertificate.policy.epoch,window.quorum.epoch);assert.equal(event.writeCertificate.policy.requiredVotes,2);assert.deepEqual(event.writeCertificate.policy.members,window.quorum.members);}
 fs.writeFileSync(path.join(output,'all-pairs-signed-events.json'),JSON.stringify(snapshot,null,2));checks.push('all resulting events verify exact configured majority certificate');
 fs.writeFileSync(path.join(output,'all-pairs.json'),JSON.stringify({ok:true,guild,checks,heads:await Promise.all(peers.map(p=>p.getRelayHead(guild)))},null,2));console.log(JSON.stringify({ok:true,checks}));
}finally{try{for(let i=0;i<3;i++)await isolate(i,false);}finally{peers.forEach(p=>p.close());}}

}
void main().catch(error=>{fs.writeFileSync(path.join(output,"all-pairs-failure.json"),JSON.stringify({ok:false,error:error.message,checks,dns:fixtureDnsObservations()},null,2));console.error(error.message);process.exitCode=1;});
