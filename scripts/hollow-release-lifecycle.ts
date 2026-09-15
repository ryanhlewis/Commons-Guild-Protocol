import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {createServer} from 'node:http';
import {generatePrivateKey,getPublicKey,hashObject,sign} from '@cgp/core';
import {createStaticShardSeedPlugin,staticShardReleaseSigningPayload,STATIC_SHARD_PUBLISHER_PROTOCOL} from '@cgp/relay/src/plugins';
import {RelayServer} from '@cgp/relay/src/server';
import {MemoryStore} from '@cgp/relay/src/store';
import {pathToFileURL} from 'node:url';

async function main() {
const run=promisify(execFile);
const workspace=path.resolve(process.cwd(),'..');
const root=await fs.mkdtemp(path.join(os.tmpdir(),'hollow-release-lifecycle-'));
const privateKey=generatePrivateKey();
const publicKey=getPublicKey(privateKey);
let relay: RelayServer;
let relayBase=''; let failCommit=true; let blobs=0;
function invariant(value: unknown, message: string): asserts value {if(!value)throw new Error(message);}
async function startRelay() {
    relay=new RelayServer(0,new MemoryStore(),[createStaticShardSeedPlugin({storeDir:path.join(root,'relay'),autoIngest:false})],{enableDefaultPlugins:false});
    for(let i=0;i<200;i++) {if(relay.getPort()>0)break;await new Promise(r=>setTimeout(r,25));}
    invariant(relay.getPort()>0,'Relay failed to bind'); relayBase=`http://127.0.0.1:${relay.getPort()}`;
}
await startRelay();
// Drop the first commit after real content uploads. Repeating the native upload
// must reuse the relay's persisted content, not re-upload all game files.
const proxy=createServer(async(req,res)=>{
    try {
        res.setHeader('access-control-allow-origin','*');
        if(req.method==='OPTIONS'){res.setHeader('access-control-allow-headers','*');res.end();return;}
        if(req.url?.endsWith('/upload-blob')) blobs++;
        if(req.url?.endsWith('/upload')&&req.method==='POST'&&failCommit){failCommit=false;req.resume();res.writeHead(503,{'content-type':'application/json'});res.end('{"error":"Injected commit interruption"}');return;}
        const chunks=[];for await(const chunk of req)chunks.push(chunk);
        const response=await fetch(`${relayBase}${req.url}`,{method:req.method,headers:{'content-type':String(req.headers['content-type']||'application/json')},body:chunks.length?Buffer.concat(chunks):undefined});
        res.writeHead(response.status,{'content-type':response.headers.get('content-type')||'application/octet-stream','access-control-allow-origin':'*'});
        res.end(Buffer.from(await response.arrayBuffer()));
    }catch(error){res.writeHead(500);res.end(String(error));}
});
await new Promise<void>(resolve=>proxy.listen(0,'127.0.0.1',resolve));
const address=proxy.address();invariant(address&&typeof address!=='string','Proxy failed');
const base=`http://127.0.0.1:${address.port}`;
async function native(spec: Record<string,unknown>) {
    const specPath=path.join(root,'spec.json');
    await fs.writeFile(specPath,JSON.stringify({...spec,root,relay:base}));
    const output=await run('cargo',['test','--manifest-path',path.join(workspace,'hollow-tauri/src-tauri/Cargo.toml'),'--lib','release_lifecycle_fixture','--','--ignored','--nocapture'],{cwd:path.join(workspace,'hollow-tauri'),env:{...process.env,HOLLOW_RELEASE_FIXTURE:specPath},maxBuffer:2*1024*1024});
    await fs.appendFile(path.join(root,'native.log'),output.stdout+output.stderr);
    return JSON.parse(await fs.readFile(path.join(root,'result.json'),'utf8'));
}
async function claim(release: any) {
    const unsigned={...release,publisher:{protocol:STATIC_SHARD_PUBLISHER_PROTOCOL,publicKey}};
    return {...unsigned.publisher,signature:await sign(privateKey,hashObject(staticShardReleaseSigningPayload(unsigned)))};
}
const releases:any[]=[];
try {
    for(const [version,compatibilityId] of [['1.0.0','garden-v1'],['1.1.0','garden-v1'],['2.0.0','']]) {
        const built=await native({action:'build',source:path.join(workspace,'hollow-svelte/examples/hollow-reference-game'),version,compatibilityId});
        invariant(built.response,`Native build failed: ${built.error}`);
        const publisher=await claim(built.response.release);
        let uploaded=await native({action:'upload',stageDir:built.response.stageDir,publisher});
        if(version==='1.0.0') {
            invariant(uploaded.error,'Commit interruption did not fail');
            const uploadedBlobs=blobs;invariant(uploadedBlobs>0,'No content uploaded');
            uploaded=await native({action:'upload',stageDir:built.response.stageDir,publisher});
            invariant(blobs===uploadedBlobs,'Retry uploaded content already stored by the relay');
        }
        invariant(uploaded.response,`Native upload failed: ${uploaded.error}`);
        const playable=await fetch(uploaded.response.playUrl);invariant(playable.ok&&(await playable.text()).includes('Hollow Garden'),'Published reference game not readable');
        releases.push(uploaded.response);
    }
    invariant(releases[0].release.network.compatibilityId===releases[1].release.network.compatibilityId,'Compatible update lost namespace');
    invariant(!releases[2].release.network?.compatibilityId,'Fresh release inherited compatibility');
    const original=releases[0];
    const listing={kind:'cgp-game-listing-update/1',id:original.gameId,version:original.version,releaseSha256:original.releaseSha256,expectedRevision:0,title:'Garden reviewed',description:'Lifecycle verified',thumbnail:''};
    const response=await fetch(`${base}/plugins/cgp.static-shards/listing`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({...listing,publisher:await claim(listing)})});
    const edited=await response.json() as any;
    invariant(response.ok&&edited.release.releaseSha256===original.releaseSha256,'Listing edit changed binary or failed');
    await relay!.close();await startRelay();
    const catalog=await (await fetch(`${base}/plugins/cgp.static-shards/catalog?id=${original.gameId}`)).json() as any;
    invariant(catalog.releases?.some((release:any)=>release.version==='2.0.0'),'Catalog did not recover after relay restart');
    // The proxy is the stable client endpoint across ephemeral relay restarts.
    for (const release of releases) { const url=new URL(release.playUrl);release.playUrl=`${base}${url.pathname}${url.search}`; }
    const summary={ok:true,root,checks:['native reference game builds','native signing/upload preparation','interrupted commit','resume without duplicate blobs','compatible and fresh release namespaces','listing preserves binary hash','relay restart restores catalog'],releases};
    // Run the actual reference game in isolated clients while the publisher relay is alive.
    const browserGate=await import(pathToFileURL(path.join(workspace,'hollow-svelte/scripts/reference-game-lifecycle.mjs')).href);
    summary['browser']=await browserGate.verifyReferenceGame(releases,base);
    await fs.writeFile(path.join(root,'summary.json'),JSON.stringify(summary,null,2));
    console.log(JSON.stringify(summary,null,2));
}finally{await relay!.close();await new Promise<void>(resolve=>proxy.close(()=>resolve()));}
}
main().then(()=>process.exit(0),error=>{console.error(error);process.exit(1);});
