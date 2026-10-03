import { it, expect } from "vitest";
import { RelayServer, LocalRelayPubSubAdapter } from "@cgp/relay/src/server";
import { MemoryStore } from "@cgp/relay/src/store";
import { generatePrivateKey,getPublicKey,hashObject,sign,computeEventId, createRelayWriteCertificate, relayWriteProposalId } from "@cgp/core";
it("rejects author-signed uncertified replication before authority or log mutation in quorum mode",async()=>{
 const keys=Array.from({length:3},()=>generatePrivateKey()); const store=new MemoryStore();
 const relay=new RelayServer(0,store,[],{enableDefaultPlugins:false,relayPrivateKeyHex:Buffer.from(keys[0]).toString("hex"),pubSubAdapter:new LocalRelayPubSubAdapter(),sequencerConsensus:false,writeQuorum:{epoch:"ingress",members:keys.map(getPublicKey),requiredVotes:2}});
 try { const owner=generatePrivateKey(),author=getPublicKey(owner),createdAt=Date.now(),body={type:"GUILD_CREATE" as const,guildId:"ingress-bypass",name:"Synthetic"};
 const event:any={seq:0,prevHash:null,createdAt,author,body,signature:await sign(owner,hashObject({body,author,createdAt}))};event.id=computeEventId(event);
 const accepted=await (relay as any).replicatePubSubEvents(body.guildId,[event]);expect(accepted).toEqual([]);expect(store.getLog(body.guildId)).toHaveLength(0);
 } finally { await relay.close(); }
});

for (const mutation of ["valid", "delayed-vote", "predated-vote", "missing", "duplicates", "wrong-epoch", "wrong-members", "weak-threshold", "malformed", "altered"]) {
 it(`quorum ingress ${mutation}`,async()=>{
  const keys=Array.from({length:4},()=>generatePrivateKey()),members=keys.slice(0,3).map(getPublicKey);
  const config={epoch:"ingress-matrix",members,requiredVotes:2}; const store=new MemoryStore();
  const relay=new RelayServer(0,store,[],{enableDefaultPlugins:false,relayPrivateKeyHex:Buffer.from(keys[0]).toString("hex"),pubSubAdapter:new LocalRelayPubSubAdapter(),sequencerConsensus:false,writeQuorum:config});
  try {
   const owner=generatePrivateKey(),author=getPublicKey(owner),createdAt=Date.now(),body={type:"GUILD_CREATE" as const,guildId:`matrix-${mutation}`,name:"Synthetic"};
   const event:any={seq:0,prevHash:null,createdAt,author,body,signature:await sign(owner,hashObject({body,author,createdAt}))};event.id=computeEventId(event);
   const proposal={guildId:body.guildId,headSeq:-1,headHash:null,body,author,signature:event.signature,createdAt};
   const policy={...config,members:[...members]};
   if(mutation==="wrong-epoch")policy.epoch="other";
   if(mutation==="wrong-members")policy.members[2]=getPublicKey(keys[3]);
   if(mutation==="weak-threshold")policy.requiredVotes=1;
   const proposalId=relayWriteProposalId(policy.epoch,proposal);
   const voteTime = createdAt + (mutation === "delayed-vote" ? 3600000 : mutation === "predated-vote" ? -3600000 : 0);
   const votes=await Promise.all(keys.slice(0,2).map(async key=>{const unsigned={protocol:"cgp/write-vote/1" as const,epoch:policy.epoch,relayPublicKey:getPublicKey(key),guildId:body.guildId,headSeq:-1,headHash:null,proposalId,votedAt:voteTime};return {...unsigned,signature:await sign(key,hashObject(unsigned))};}));
   event.writeCertificate=createRelayWriteCertificate(policy,proposal,votes);
   if(mutation==="missing")delete event.writeCertificate;
   if(mutation==="duplicates")event.writeCertificate.votes=[votes[0],votes[0]];
   if(mutation==="malformed")event.writeCertificate.votes=null;
   if(mutation==="altered")event.body.name="tampered";
   let authorizationCalls=0;const original=(relay as any).verifyAccountAuthorization.bind(relay);(relay as any).verifyAccountAuthorization=(...args:any[])=>{authorizationCalls++;return original(...args);};
   const accepted=await (relay as any).replicatePubSubEvents(body.guildId,[event]);
   const valid = mutation === "valid" || mutation === "delayed-vote";
   expect(accepted).toHaveLength(valid?1:0);expect(store.getLog(body.guildId)).toHaveLength(valid?1:0);
   expect(authorizationCalls).toBe(valid?1:0);
   await expect((relay as any).appendEventsFromPlugin([event])).rejects.toThrow("write quorum");
  } finally {await relay.close();}
 });
}
it("preserves uncertified replication for no-quorum deployments",async()=>{
 const store=new MemoryStore(),relay=new RelayServer(0,store,[],{enableDefaultPlugins:false,sequencerConsensus:false});
 try {const priv=generatePrivateKey(),author=getPublicKey(priv),createdAt=Date.now(),body={type:"GUILD_CREATE" as const,guildId:"legacy",name:"Legacy"};const event:any={seq:0,prevHash:null,createdAt,author,body,signature:await sign(priv,hashObject({body,author,createdAt}))};event.id=computeEventId(event);expect(await (relay as any).replicatePubSubEvents(body.guildId,[event])).toHaveLength(1);}finally{await relay.close();}
});
