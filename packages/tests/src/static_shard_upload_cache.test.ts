import { describe, it, expect } from 'vitest';
import { mkdtemp, mkdir, writeFile, readFile, rm, rmdir, utimes, symlink, lstat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { StaticShardUploadCache } from '../../relay/src/static_shard_upload_cache';
import { reassembleStaticFiles } from '../../relay/src/static_shard_chunks';

const hash = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex');
describe('resumable game content', () => {
  it('does not reclaim pending symlinks, directories or modified copies',async()=>{
    const root=await mkdtemp(path.join(tmpdir(),'hollow-upload-pending-types-'));
    try {
      const a=Buffer.from('authentic'),key=hash(a),target=path.join(root,'target');await writeFile(target,a);
      const cache=new StaticShardUploadCache(root,16,async h=>h===key?a:undefined);
      await symlink(target,path.join(root,key));
      expect(await cache.reclaimCommitted()).toEqual([]);expect((await lstat(path.join(root,key))).isSymbolicLink()).toBe(true);
      await rm(path.join(root,key));await mkdir(path.join(root,key));expect(await cache.reclaimCommitted()).toEqual([]);
      await rmdir(path.join(root,key));await writeFile(path.join(root,key),'modified!');expect(await cache.reclaimCommitted()).toEqual([]);
      expect(await readFile(target)).toEqual(a);
    }finally{await rm(root,{recursive:true,force:true});}
  });
  it('reclaims only authenticated committed duplicates and serves concurrent references durably',async()=>{
    const root=await mkdtemp(path.join(tmpdir(),'hollow-upload-committed-'));
    try {
      const a=Buffer.from('aaaa'),b=Buffer.from('bbbb'),c=Buffer.from('cccc');
      const committed=new Map<string,Buffer>();
      const cache=new StaticShardUploadCache(root,8,async key=>committed.get(key));
      await cache.putBatch([{hash:hash(a),bytes:a},{hash:hash(b),bytes:b}]);
      await expect(cache.put(hash(c),c)).rejects.toThrow('full');
      // A verified durable resolver proves only A. A wrong digest cannot authorize B removal.
      committed.set(hash(a),a);committed.set(hash(b),c);
      expect(await cache.reclaimCommitted()).toEqual([hash(a)]);
      await expect(readFile(path.join(root,hash(a)))).rejects.toMatchObject({code:'ENOENT'});
      expect(await readFile(path.join(root,hash(b)))).toEqual(b);
      expect(await cache.get(hash(a))).toEqual(a);expect(await cache.size(hash(a))).toBe(4);
      await cache.put(hash(c),c);
      expect(await cache.get(hash(a))).toEqual(a);
      await expect(cache.put(hash(Buffer.from('x')),Buffer.from('x'))).rejects.toThrow('full');
      committed.delete(hash(a));expect(await cache.get(hash(a))).toBeUndefined();
    }finally{await rm(root,{recursive:true,force:true});}
  });
  it('reserves the entire deduplicated batch under concurrent uploads and reuses verified cached blobs',async()=>{
    const root=await mkdtemp(path.join(tmpdir(),'hollow-upload-batch-'));
    try{const cache=new StaticShardUploadCache(root,8),a=Buffer.from('1234'),b=Buffer.from('5678'),c=Buffer.from('x');
      await cache.putBatch([{hash:hash(a),bytes:a},{hash:hash(b),bytes:b},{hash:hash(a),bytes:a}]);
      await cache.putBatch([{hash:hash(a),bytes:a},{hash:hash(b),bytes:b}]);
      await expect(cache.putBatch([{hash:hash(c),bytes:c}])).rejects.toThrow('full');
      expect(await cache.get(hash(a))).toEqual(a);expect(await cache.get(hash(b))).toEqual(b);
    }finally{await rm(root,{recursive:true,force:true});}
  });
  it('rejects a changed batch digest before any writes and expires old cache bytes before reservation',async()=>{
    const root=await mkdtemp(path.join(tmpdir(),'hollow-upload-batch-expiry-'));
    try{const cache=new StaticShardUploadCache(root,6),old=Buffer.from('old!'),a=Buffer.from('abc'),b=Buffer.from('def');
      await expect(cache.putBatch([{hash:hash(a),bytes:a},{hash:hash(b),bytes:a}])).rejects.toThrow('hash mismatch');
      expect(await cache.get(hash(a))).toBeUndefined();
      await cache.put(hash(old),old);const expired=new Date(Date.now()-48*3600000);await utimes(path.join(root,hash(old)),expired,expired);
      await cache.putBatch([{hash:hash(a),bytes:a},{hash:hash(b),bytes:b}]);
      expect(await cache.get(hash(old))).toBeUndefined();expect(await cache.get(hash(a))).toEqual(a);expect(await cache.get(hash(b))).toEqual(b);
    }finally{await rm(root,{recursive:true,force:true});}
  });
  it('reuses verified content after restart and detects corrupted cache entries', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'hollow-upload-cache-'));
    try {
      const bytes = Buffer.from('game content'), key = hash(bytes);
      const cache = new StaticShardUploadCache(root);
      await cache.put(key, bytes);
      await cache.put(key, bytes);
      expect(await new StaticShardUploadCache(root).get(key)).toEqual(bytes);
      await writeFile(path.join(root, key), 'corrupted');
      expect(await cache.get(key)).toBeUndefined();
      await expect(cache.put(key, Buffer.from('wrong'))).rejects.toThrow('hash mismatch');
    } finally { await rm(root, {recursive: true, force: true}); }
  });
  it('enforces a cache quota under concurrent uploads', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'hollow-upload-quota-'));
    try {
      const cache = new StaticShardUploadCache(root, 4);
      const a = Buffer.from('1234'), b = Buffer.from('5678');
      const results = await Promise.allSettled([cache.put(hash(a), a), cache.put(hash(b), b)]);
      expect(results.map(r => r.status)).toEqual(['fulfilled', 'rejected']);
    } finally { await rm(root, {recursive: true, force: true}); }
  });
  it('reconstructs large files and rejects traversal, wrong hashes and duplicate parts', async () => {
    const root = await mkdtemp(path.join(tmpdir(), 'hollow-reassemble-'));
    try {
      await mkdir(path.join(root, '__hollow_chunks'));
      const a = Buffer.from('first'), b = Buffer.from('second');
      await writeFile(path.join(root, '__hollow_chunks/a'), a);
      await writeFile(path.join(root, '__hollow_chunks/b'), b);
      const entry = {path:'game.data', bytes: a.length+b.length, sha256:hash(Buffer.concat([a,b])), parts:['__hollow_chunks/a','__hollow_chunks/b']};
      await expect(reassembleStaticFiles(root, [{...entry,path:'../escape'}], 100, 10)).rejects.toThrow('Unsafe');
      await expect(reassembleStaticFiles(root, [{...entry,sha256:'0'.repeat(64)}], 100, 10)).rejects.toThrow('integrity');
      await expect(reassembleStaticFiles(root, [{...entry,parts:['__hollow_chunks/a','__hollow_chunks/a']}], 100, 10)).rejects.toThrow('reused');
      await reassembleStaticFiles(root, [entry], 100, 10);
      expect(await readFile(path.join(root, 'game.data'))).toEqual(Buffer.concat([a,b]));
    } finally { await rm(root, {recursive: true, force: true}); }
  });
});
