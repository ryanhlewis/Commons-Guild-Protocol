import {createHash,randomUUID} from 'node:crypto';
import {readFile,writeFile,rename,mkdir} from 'node:fs/promises';
import path from 'node:path';
type RecordEntry={cid:string;sha256:string;bytes:number;mimeType:string};
/** A durable media index over genuine, pinned raw IPFS blocks. Every read verifies the CID digest. */
export class RawPinnedMedia {
 private entries=new Map<string,RecordEntry>();private loaded?:Promise<void>;private writes=Promise.resolve();
 constructor(private storeDir:string,private importModule:(name:string)=>Promise<any>){}
 private load(){return this.loaded??=(async()=>{try{const rows=JSON.parse(await readFile(path.join(this.storeDir,'public-raw-pins.json'),'utf8'));for(const r of rows)if(typeof r.cid==='string'&&/^[a-f0-9]{64}$/.test(r.sha256)&&Number.isSafeInteger(r.bytes)&&r.bytes>0&&r.bytes<=25*1024*1024&&/^image\/[a-z0-9.+-]+$/i.test(r.mimeType))this.entries.set(r.cid,r);}catch(e:any){if(e.code!=='ENOENT')throw e;}})();}
 async register(entry:RecordEntry){await this.load();const {CID}=await this.importModule('multiformats/cid');const cid=CID.parse(entry.cid);if(cid.code!==0x55||cid.multihash.code!==0x12||Buffer.from(cid.multihash.digest).toString('hex')!==entry.sha256||!/^image\/[a-z0-9.+-]+$/i.test(entry.mimeType))return;
  this.entries.set(entry.cid,entry);this.writes=this.writes.catch(()=>{}).then(async()=>{await mkdir(this.storeDir,{recursive:true});const temp=path.join(this.storeDir,'public-raw-pins.'+randomUUID()+'.tmp');await writeFile(temp,JSON.stringify([...this.entries.values()]));await rename(temp,path.join(this.storeDir,'public-raw-pins.json'));});await this.writes;
 }
 async read(cidValue:string){await this.load();const entry=this.entries.get(cidValue);if(!entry)return null;const [{CID},{base32upper}]=await Promise.all([this.importModule('multiformats/cid'),this.importModule('multiformats/bases/base32')]);const cid=CID.parse(cidValue);if(cid.code!==0x55||cid.multihash.code!==0x12)throw Error('Invalid registered raw media CID');const encoded=base32upper.encode(cid.multihash.bytes);const bytes=await readFile(path.join(this.storeDir,'blocks',encoded.slice(-2),encoded+'.data'));const sha=createHash('sha256').update(bytes).digest('hex');if(bytes.length!==entry.bytes||sha!==entry.sha256||sha!==Buffer.from(cid.multihash.digest).toString('hex'))throw Error('Pinned media CID integrity mismatch');return{...entry,bytes};}
}
