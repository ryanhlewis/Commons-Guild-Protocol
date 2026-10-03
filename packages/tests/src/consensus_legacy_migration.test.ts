import { expect, it } from 'vitest';
import { generatePrivateKey, getPublicKey, hashObject, sign, consensusPolicyHash,
 legacyConsensusPolicyHash, legacyConsensusRequestPayload, verifyLegacyConsensusBridge,
 verifyConsensusHistory, type LegacyConsensusTrust, type LegacyConsensusMigrationRequest,
 type LegacyConsensusBridge, type ConsensusHistory, type ConsensusPolicy, type RelayWriteProposal } from '@cgp/core';
import { MemoryStore } from '@cgp/relay/src/store';
import { RelayWriteQuorumCoordinator } from '@cgp/relay/src/write_quorum';
import { RelayConsensusCoordinator } from '@cgp/relay/src/consensus_v2';

async function fixture() {
 const keys=Array.from({length:3},()=>generatePrivateKey()), pubs=keys.map(getPublicKey), owner=generatePrivateKey();
 const trust:LegacyConsensusTrust={policy:{epoch:'legacy',members:pubs,requiredVotes:2},administrators:[getPublicKey(owner)],requiredAdministrators:1};
 const anchor:ConsensusPolicy={...trust.policy,epoch:'new',administrators:trust.administrators,requiredAdministrators:1};
 const stores=keys.map(()=>new MemoryStore());
 const transport={publish(){},subscribe(){return ()=>{};}};
 const create=(i:number)=>new RelayWriteQuorumCoordinator({...trust.policy,voteTimeoutMs:100},{publicKey:pubs[i],privateKey:keys[i]},stores[i],transport);
 const nodes=keys.map((_,i)=>create(i));
 const unsigned={protocol:'cgp/legacy-migration-request/2' as const,guildId:'guild',legacyPolicyHash:legacyConsensusPolicyHash(trust),nextPolicyHash:consensusPolicyHash(anchor),nonce:'explicit-migration'};
 const request:LegacyConsensusMigrationRequest={...unsigned,signatures:[{publicKey:getPublicKey(owner),signature:await sign(owner,hashObject(unsigned))}]};
 const proposal=async(name:string):Promise<RelayWriteProposal>=>{
  const body={type:'GUILD_CREATE' as const,guildId:'guild',name}, payload={body,author:getPublicKey(owner),createdAt:Date.now()};
  return {...payload,guildId:'guild',headSeq:-1,headHash:null,clientEventId:name,signature:await sign(owner,hashObject(payload))};
 };
 const freeze=()=>Promise.all(nodes.map(node=>node.freezeGuild(request,trust,anchor,()=>[])));
 return {keys,pubs,owner,trust,anchor,stores,nodes,create,request,proposal,freeze};
}
it('all-voter legacy freeze safely supersedes conflicting partial fences without erasing them',async()=>{
 const f=await fixture();try{
  for(let i=0;i<3;i++)await expect(f.nodes[i].authorize(await f.proposal('partial-'+i))).rejects.toThrow('quorum unavailable');
  const bridge:LegacyConsensusBridge={protocol:'cgp/legacy-bridge/2',request:f.request,freezes:await f.freeze(),events:[]};
  expect(verifyLegacyConsensusBridge(bridge,'guild',f.anchor,f.trust)).toEqual([]);
  const history:ConsensusHistory={protocol:'cgp/consensus-history/2',guildId:'guild',anchorPolicy:f.anchor,base:bridge,entries:[]};
  const v2=new RelayConsensusCoordinator('guild',f.anchor,{publicKey:f.pubs[0],privateKey:f.keys[0]},f.stores[0],{validateEvent:()=>true,materialize(){}},f.trust);
  await expect(v2.nextPrepareRequest()).rejects.toThrow('migration bridge');
  await v2.sync(history);expect((await v2.nextPrepareRequest()).protocol).toBe('cgp/consensus-prepare/2');
  await f.nodes[0].close();f.nodes[0]=f.create(0);
  await expect(f.nodes[0].authorize(await f.proposal('late'))).rejects.toThrow('durably frozen');
  expect(verifyConsensusHistory(history,'guild',f.anchor,f.trust).events).toHaveLength(0);
  expect(()=>verifyConsensusHistory(history,'guild',f.anchor)).toThrow('trusted legacy');
 }finally{await Promise.all(f.nodes.map(n=>n.close()));}
});
it('requires exact signed payload for any legacy fence that may already have reached a majority',async()=>{
 const f=await fixture();try{
  const pending=await f.proposal('possibly-chosen');
  await Promise.all(f.nodes.slice(0,2).map(node=>expect(node.authorize(pending)).rejects.toThrow('quorum unavailable')));
  const bridge:LegacyConsensusBridge={protocol:'cgp/legacy-bridge/2',request:f.request,freezes:await f.freeze(),events:[]};
  expect(()=>verifyLegacyConsensusBridge(bridge,'guild',f.anchor,f.trust)).toThrow('Exact potentially chosen');
  const recovered=verifyLegacyConsensusBridge({...bridge,pendingProposal:pending},'guild',f.anchor,f.trust);
  expect(recovered).toHaveLength(1);expect(recovered[0].body).toEqual(pending.body);expect(recovered[0].writeCertificate).toBeUndefined();
  expect(()=>verifyLegacyConsensusBridge({...bridge,pendingProposal:{...pending,clientEventId:'altered'}},'guild',f.anchor,f.trust)).toThrow('Exact potentially chosen');
 }finally{await Promise.all(f.nodes.map(n=>n.close()));}
});
it('rejects missing/duplicated old voters, unauthorized migration, and changed certified heads',async()=>{
 const f=await fixture();try{
  const freezes=await f.freeze(),bridge:LegacyConsensusBridge={protocol:'cgp/legacy-bridge/2',request:f.request,freezes,events:[]};
  expect(()=>verifyLegacyConsensusBridge({...bridge,freezes:freezes.slice(0,2)},'guild',f.anchor,f.trust)).toThrow('Every old voter');
  expect(()=>verifyLegacyConsensusBridge({...bridge,freezes:[freezes[0],freezes[0],freezes[2]]},'guild',f.anchor,f.trust)).toThrow('inventory');
  expect(()=>verifyLegacyConsensusBridge({...bridge,request:{...f.request,nonce:'tampered'}},'guild',f.anchor,f.trust)).toThrow('authorized');
  expect(()=>verifyLegacyConsensusBridge({...bridge,freezes:[{...freezes[0],headSeq:0},...freezes.slice(1)]},'guild',f.anchor,f.trust)).toThrow('inventory');
 }finally{await Promise.all(f.nodes.map(n=>n.close()));}
});
it('freeze waits for an in-flight durable vote and records it before denying all later votes',async()=>{
 const f=await fixture();let release!:()=>void,entered!:()=>void;
 const enteredPromise=new Promise<void>(r=>{entered=r;}),gate=new Promise<void>(r=>{release=r;});
 const original=f.stores[0].putWriteVoteFence.bind(f.stores[0]);
 f.stores[0].putWriteVoteFence=async(key,value)=>{entered();await gate;original(key,value);};
 try{
  const proposal=await f.proposal('in-flight');const voting=f.nodes[0].authorize(proposal).catch(error=>error);await enteredPromise;
  let completed=false;const freezing=f.nodes[0].freezeGuild(f.request,f.trust,f.anchor,()=>[]).then(value=>{completed=true;return value;});
  await Promise.resolve();await Promise.resolve();expect(completed).toBe(false);
  release();const freeze=await freezing;expect(freeze.fenceProposalId).toMatch(/^[a-f0-9]{64}$/);
  await expect(f.nodes[0].authorize(await f.proposal('after-freeze'))).rejects.toThrow('durably frozen');
  expect(await voting).toBeInstanceOf(Error);
 }finally{release?.();await Promise.all(f.nodes.map(n=>n.close()));}
});
