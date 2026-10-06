import {open,lstat} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {inflateRawSync} from 'node:zlib';

/** Offline candidate: returns only bytes matching a signed plaintext entry binding. */
export async function readTrustedZipEntry(archive:string,name:string,binding:{bytes:number;sha256:string},limits={entryBytes:8*1024**2,compressedBytes:8*1024**2,directoryBytes:8*1024**2,entries:100000}):Promise<Buffer>{
 if(!Number.isSafeInteger(binding.bytes)||binding.bytes<0||binding.bytes>limits.entryBytes||! /^[a-f0-9]{64}$/.test(binding.sha256)||!name||name.includes('\\')||name.startsWith('/')||name.split('/').some(s=>s==='..'||s==='.'||!s))throw Error('Invalid ZIP entry binding');
 const stat=await lstat(archive);if(!stat.isFile()||stat.isSymbolicLink())throw Error('ZIP archive must be a regular file');
 const file=await open(archive,'r');
 try{
  const actual=await file.stat();if(actual.size!==stat.size||actual.ino!==stat.ino||actual.size<22||actual.size>0xffffffff)throw Error('ZIP archive changed or is unsupported');
  const read=async(offset:number,size:number)=>{if(!Number.isSafeInteger(offset)||!Number.isSafeInteger(size)||offset<0||size<0||offset+size>actual.size)throw Error('ZIP range outside archive');const b=Buffer.alloc(size);let done=0;while(done<size){const r=await file.read(b,done,size-done,offset+done);if(!r.bytesRead)throw Error('ZIP archive truncated');done+=r.bytesRead;}return b;};
  const tail=await read(Math.max(0,actual.size-65557),Math.min(actual.size,65557));let end=-1;
  for(let i=tail.length-22;i>=0;i--)if(tail.readUInt32LE(i)===0x06054b50&&i+22+tail.readUInt16LE(i+20)===tail.length){end=i;break;}
  if(end<0||tail.readUInt16LE(end+4)!==0||tail.readUInt16LE(end+6)!==0)throw Error('Invalid ZIP end record');
  const count=tail.readUInt16LE(end+10),directorySize=tail.readUInt32LE(end+12),directoryOffset=tail.readUInt32LE(end+16);
  if(count===65535||count!==tail.readUInt16LE(end+8)||count>limits.entries||directorySize>limits.directoryBytes||directoryOffset+directorySize>actual.size-tail.length+end)throw Error('ZIP directory limit or unsupported ZIP64');
  const directory=await read(directoryOffset,directorySize);let cursor=0,found: {offset:number;compressed:number;method:number;flags:number}|undefined;
  for(let i=0;i<count;i++){
   if(cursor+46>directory.length||directory.readUInt32LE(cursor)!==0x02014b50)throw Error('Malformed ZIP directory');
   const flags=directory.readUInt16LE(cursor+8),method=directory.readUInt16LE(cursor+10),compressed=directory.readUInt32LE(cursor+20),bytes=directory.readUInt32LE(cursor+24),n=directory.readUInt16LE(cursor+28),extra=directory.readUInt16LE(cursor+30),comment=directory.readUInt16LE(cursor+32),next=cursor+46+n+extra+comment;
   if(next>directory.length)throw Error('Malformed ZIP entry');
   if(directory.subarray(cursor+46,cursor+46+n).toString('utf8')===name){if(found||flags&1||! [0,8].includes(method)||bytes!==binding.bytes||compressed>limits.compressedBytes||directory.readUInt16LE(cursor+34)!==0)throw Error('Unsafe or conflicting ZIP entry');found={offset:directory.readUInt32LE(cursor+42),compressed,method,flags};}
   cursor=next;
  }
  if(cursor!==directory.length||!found)throw Error('ZIP entry not found or trailing directory data');
  const header=await read(found.offset,30);if(header.readUInt32LE(0)!==0x04034b50||header.readUInt16LE(6)!==found.flags||header.readUInt16LE(8)!==found.method)throw Error('ZIP local header mismatch');
  const localNameSize=header.readUInt16LE(26),localExtraSize=header.readUInt16LE(28),localName=await read(found.offset+30,localNameSize);
  if(localName.toString('utf8')!==name)throw Error('ZIP local name mismatch');
  const dataOffset=found.offset+30+localNameSize+localExtraSize;if(dataOffset+found.compressed>directoryOffset)throw Error('ZIP payload overlaps directory');
  const compressed=await read(dataOffset,found.compressed),bytes=found.method===0?compressed:inflateRawSync(compressed,{maxOutputLength:Math.max(1,binding.bytes)});
  if(bytes.length!==binding.bytes||createHash('sha256').update(bytes).digest('hex')!==binding.sha256)throw Error('ZIP entry authentication failed');
  return bytes;
 }finally{await file.close();}
}
