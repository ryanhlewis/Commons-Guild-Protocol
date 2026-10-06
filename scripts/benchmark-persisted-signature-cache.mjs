import fs from 'node:fs/promises';
import { performance } from 'node:perf_hooks';
import * as secp from '@noble/secp256k1';
import { generatePrivateKey, getPublicKey, sign, verify, DeviceAuthorityRegistry, signatureVerificationCacheStatistics } from '../packages/core/dist/index.js';
const root=process.argv[2];
if(!root)throw Error('Explicit registry directory required');
const snapshot=JSON.parse(await fs.readFile(root+'/index.json','utf8'));
const journal=(await fs.readFile(root+'/index.json.ndjson','utf8')).split(/\r?\n/).filter(Boolean).map(line=>JSON.parse(line));
const records=[...(snapshot.releases??[]),...journal].filter(value=>typeof value.id==='string'&&typeof value.version==='string'&&Array.isArray(value.shards));
const proofs=records.filter(value=>value.publisher).map(value=>value.publisher);
const distinct=new Set(proofs.map(proof=>JSON.stringify(proof)));
if(process.argv.includes('--replay')){
 const authority=new DeviceAuthorityRegistry(100000);let accepted=0,rejected=0;
 const before=signatureVerificationCacheStatistics(),started=performance.now();
 for(const proof of proofs){
  const valid=proof.deviceAuthorization?authority.verify(proof.payloadHash,proof.signature,proof.publicKey,proof.deviceAuthorization,'publish').ok:!authority.get(proof.publicKey)&&verify(proof.publicKey,proof.payloadHash,proof.signature);
  valid?accepted++:rejected++;
  if((accepted+rejected)%8===0)await new Promise(resolve=>setImmediate(resolve));
 }
 console.log(JSON.stringify({protocol:'cgp-local-publisher-cache-replay/1',publisherProofRecords:proofs.length,distinctExactPublisherProofs:distinct.size,accepted,rejected,durationMs:performance.now()-started,cacheBefore:before,cacheAfter:signatureVerificationCacheStatistics(),networkRequests:0,liveRelayModified:false}));
 process.exit(0);
}
const key=generatePrivateKey(),publicKey=getPublicKey(key),hash='ab'.repeat(32),signature=await sign(key,hash);
const count=200;
let started=performance.now();for(let i=0;i<count;i++)if(!secp.verify(signature,hash,publicKey))throw Error('Signature invalid');const uncachedMs=performance.now()-started;
started=performance.now();for(let i=0;i<count;i++)if(!verify(publicKey,hash,signature))throw Error('Signature invalid');const cachedMs=performance.now()-started;
console.log(JSON.stringify({publisherProofRecords:proofs.length,distinctExactPublisherProofs:distinct.size,duplicateExactPublisherProofs:proofs.length-distinct.size,benchmark:{count,uncachedMs,cachedMs},note:'Local cryptographic microbenchmark; no authorization decisions bypassed and no claim of live cold-start timing.'}));
