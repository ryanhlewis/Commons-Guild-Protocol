import { describe, it, expect } from 'vitest';
import { mkdtemp, mkdir, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { StaticShardUploadCache } from '../../relay/src/static_shard_upload_cache';
import { reassembleStaticFiles } from '../../relay/src/static_shard_chunks';

const hash = (bytes: Buffer) => createHash('sha256').update(bytes).digest('hex');
describe('resumable game content', () => {
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
