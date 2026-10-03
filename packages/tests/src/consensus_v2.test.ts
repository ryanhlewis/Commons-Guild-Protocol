import { expect, it } from 'vitest';
import { generatePrivateKey, getPublicKey, hashObject, sign, computeEventId, consensusUnsigned,
 consensusTransitionPayload, consensusPolicyHash, consensusCommitHash, verifyConsensusHistory,
 type ConsensusValue, type ConsensusAcceptRequest, type ConsensusPolicy } from '@cgp/core';
import { RelayConsensusCoordinator, type ConsensusTransport } from '@cgp/relay/src/consensus_v2';
import { MemoryStore } from '@cgp/relay/src/store';

async function fixture() {
 const keys=Array.from({length:4},()=>generatePrivateKey()), pubs=keys.map(getPublicKey), owner=generatePrivateKey();
 const policy:ConsensusPolicy={epoch:'first',members:pubs.slice(0,3),requiredVotes:2,administrators:[getPublicKey(owner)],requiredAdministrators:1};
 const stores=keys.map(()=>new MemoryStore()), logs=keys.map(()=>[] as any[]);
 const create=(i:number)=>new RelayConsensusCoordinator('guild',policy,{publicKey:pubs[i],privateKey:keys[i]},stores[i],{
  validateEvent:(event)=>event.author===getPublicKey(owner), materialize:events=>{logs[i]=events;},
 });
 const nodes=keys.map((_,i)=>create(i));
 const unavailable=new Set<string>();
 const transport:ConsensusTransport={async call(peer,method,payload){if(unavailable.has(peer))throw Error('partition');return (nodes[pubs.indexOf(peer)][method] as any)(payload);}};
 const value=async(name:string,seq=0,prevHash:string|null=null):Promise<ConsensusValue>=>{
  const body:any=seq===0?{type:'GUILD_CREATE',guildId:'guild',name}:{type:'CHANNEL_CREATE',guildId:'guild',channelId:name,name,kind:'text'};
  const unsigned={body,author:getPublicKey(owner),createdAt:Date.now()}; const event:any={...unsigned,seq,prevHash,signature:await sign(owner,hashObject(unsigned))};event.id=computeEventId(event);return {kind:'event',event};
 };
 const accept=async(proposer:number,request:any,promises:any[],v:ConsensusValue):Promise<ConsensusAcceptRequest>=>{
  const unsigned={protocol:'cgp/consensus-accept/2' as const,guildId:request.guildId,index:request.index,parentHash:request.parentHash,policyHash:request.policyHash,ballot:request.ballot,value:v,promises};
  return {...unsigned,signature:await sign(keys[proposer],hashObject(unsigned))};
 };
 return {keys,pubs,owner,policy,stores,logs,nodes,create,transport,unavailable,value,accept};
}
it('recovers conflicting partial accepts by carrying the highest accepted value after restart',async()=>{
 const f=await fixture(),a=await f.value('A'),b=await f.value('B'),c=await f.value('C');
 const one=await f.nodes[0].nextPrepareRequest();const promises1=await Promise.all(f.nodes.slice(0,3).map(n=>n.prepare(one)));
 await f.nodes[0].accept(await f.accept(0,one,promises1,a));
 const two=await f.nodes[1].nextPrepareRequest(one.ballot.counter);const promises2=await Promise.all([f.nodes[1].prepare(two),f.nodes[2].prepare(two)]);
 await f.nodes[1].accept(await f.accept(1,two,promises2,b));
 f.nodes[0]=f.create(0);f.nodes[1]=f.create(1);
 const result=await f.nodes[2].propose(c,f.transport,two.ballot.counter);
 expect(result.recoveredDifferentValue).toBe(true);expect(result.commit.value).toEqual(b);
 expect(f.logs.slice(0,3).map(log=>log[0].id)).toEqual([ (b as any).event.id,(b as any).event.id,(b as any).event.id]);
});
it('refuses a malicious proposer that discards the prepare quorum highest accepted value',async()=>{
 const f=await fixture(),a=await f.value('A'),b=await f.value('B');const one=await f.nodes[0].nextPrepareRequest();
 const p=await Promise.all(f.nodes.slice(0,3).map(n=>n.prepare(one)));await f.nodes[0].accept(await f.accept(0,one,p,a));
 const two=await f.nodes[1].nextPrepareRequest(one.ballot.counter);const p2=await Promise.all(f.nodes.slice(0,3).map(n=>n.prepare(two)));
 await expect(f.nodes[2].accept(await f.accept(1,two,p2,b))).rejects.toThrow('highest accepted');
});
it('shared store coordinators cannot equivocate on a ballot',async()=>{
 const f=await fixture(),other=f.create(0),one=await f.nodes[0].nextPrepareRequest();const p=await Promise.all(f.nodes.slice(0,3).map(n=>n.prepare(one)));
 const [a,b]=await Promise.all([f.accept(0,one,p,await f.value('A')),f.accept(0,one,p,await f.value('B'))]);
 const results=await Promise.allSettled([f.nodes[0].accept(a),other.accept(b)]);
 expect(results.map(r=>r.status)).toEqual(['fulfilled','rejected']);
});
it('requires authenticated current voter prepares and keeps promises across restart',async()=>{
 const f=await fixture(),one=await f.nodes[0].nextPrepareRequest(10);await f.nodes[1].prepare(one);f.nodes[1]=f.create(1);
 const stale=await f.nodes[2].nextPrepareRequest();await expect(f.nodes[1].prepare(stale)).rejects.toThrow('durable promise');
 await expect(f.nodes[1].prepare({...one,signature:'00'.repeat(64)})).rejects.toThrow('authenticated');
});
it('joint transition retires the old epoch, verifies catchup, and continues app sequence under new membership',async()=>{
 const f=await fixture();const first=await f.nodes[0].propose(await f.value('genesis'),f.transport);
 const scope=await f.nodes[0].scope();const next={...f.policy,epoch:'second',members:[f.pubs[1],f.pubs[2],f.pubs[3]]};
 const transition:any={kind:'transition',guildId:'guild',fromPolicyHash:scope.policyHash,parentHash:scope.parentHash,nextPolicy:next,nonce:'transition-one',signatures:[]};
 transition.signatures=[{publicKey:getPublicKey(f.owner),signature:await sign(f.owner,hashObject(consensusTransitionPayload(transition)))}];
 const stop=await f.nodes[0].propose(transition,f.transport);expect(stop.commit.value.kind).toBe('transition');
 await expect(f.nodes[3].ready()).rejects.toThrow('No certified transition');
 f.nodes[0]=f.create(0);await expect(f.nodes[0].nextPrepareRequest()).rejects.toThrow('cannot propose');
 f.unavailable.add(f.pubs[2]);f.unavailable.add(f.pubs[3]);
 await expect(f.nodes[0].activateTransition(f.transport)).rejects.toThrow('readiness quorum');
 f.unavailable.clear();await f.nodes[0].activateTransition(f.transport);
 const event=(first.commit.value as any).event;
 const committed=await f.nodes[1].propose(await f.value('after',1,event.id),f.transport);
 expect(committed.commit.index).toBe(2);expect((committed.commit.value as any).event.seq).toBe(1);
 const verified=verifyConsensusHistory(committed.history,'guild',f.policy);expect(verified.policy.epoch).toBe('second');expect(verified.events).toHaveLength(2);
 await expect(f.nodes[0].nextPrepareRequest()).rejects.toThrow('cannot propose');
});
it('rejects prefix divergence, missing activation, invalid administrative transition and stale history',async()=>{
 const f=await fixture(),first=await f.nodes[0].propose(await f.value('genesis'),f.transport),history=await f.nodes[0].history();
 const scope=await f.nodes[0].scope(),transition:any={kind:'transition',guildId:'guild',fromPolicyHash:scope.policyHash,parentHash:scope.parentHash,nextPolicy:{...f.policy,epoch:'next'},nonce:'x',signatures:[]};
 await expect(f.nodes[0].propose(transition,f.transport)).rejects.toThrow('Invalid proposed');
 const altered=structuredClone(history);(altered.entries[0].commit.value as any).event.body.name='tampered';await expect(f.nodes[1].sync(altered)).rejects.toThrow('certificate');
 await expect(f.nodes[1].sync({...history,entries:[]})).rejects.toThrow('rollback');
 expect(consensusCommitHash(first.commit)).toBe(consensusCommitHash({...first.commit,votes:[...first.commit.votes].reverse()}));
});

it('retains a durable promise when the process fails after persistence but before acknowledgement',async()=>{
 const f=await fixture(),request=await f.nodes[0].nextPrepareRequest(20);
 const original=f.stores[1].putConsensusState.bind(f.stores[1]);
 f.stores[1].putConsensusState=stateWriter;
 function stateWriter(guildId:string,state:any){original(guildId,state);throw Error('simulated crash after durable write');}
 await expect(f.nodes[1].prepare(request)).rejects.toThrow('simulated crash');
 f.stores[1].putConsensusState=original;f.nodes[1]=f.create(1);
 const older=await f.nodes[2].nextPrepareRequest();
 await expect(f.nodes[1].prepare(older)).rejects.toThrow('durable promise');
});
it('does not trust a previously verified caller object after it is mutated',async()=>{
 const f=await fixture();const result=await f.nodes[0].propose(await f.value('genesis'),f.transport);
 await f.nodes[3].sync(result.history);
 (result.history.entries[0].commit.value as any).event.body.name='mutated-caller';
 const saved=await f.nodes[3].history();
 expect((saved.entries[0].commit.value as any).event.body.name).toBe('genesis');
 await expect(f.nodes[3].sync(result.history)).rejects.toThrow('certificate');
});
