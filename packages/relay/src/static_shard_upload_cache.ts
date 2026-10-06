import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, readdir, rename, rm, stat, lstat, writeFile } from 'node:fs/promises';
import path from 'node:path';

/** Disposable cache: the signed release remains the authority for every reference. */
export class StaticShardUploadCache {
  private queue: Promise<unknown> = Promise.resolve();
  constructor(private root: string, private maxBytes = 2 * 1024 ** 3,
    private durable?: (hash: string) => Promise<Buffer | undefined>) {}
  private file(hash: string) {
    if (!/^[a-f0-9]{64}$/.test(hash)) throw new Error('Invalid upload content hash.');
    return path.join(this.root, hash);
  }
  async get(hash: string): Promise<Buffer | undefined> {
    try {
      const bytes = await readFile(this.file(hash));
      if (createHash('sha256').update(bytes).digest('hex') !== hash) return undefined;
      return bytes;
    } catch (error: any) { if (error.code !== 'ENOENT') throw error; }
    const durable = await this.durable?.(hash);
    return durable && createHash('sha256').update(durable).digest('hex') === hash ? durable : undefined;
  }
  async size(hash: string) { try { return (await stat(this.file(hash))).size; } catch(error:any) { if(error.code!=='ENOENT') throw error; } return (await this.get(hash))?.length; }
  /** Remove only exact pending copies independently authenticated in a committed release. */
  async reclaimCommitted(): Promise<string[]> {
    const operation = this.queue.then(async () => {
      const removed: string[] = [];
      if (!this.durable) return removed;
      let names: string[];
      try { names = await readdir(this.root); } catch (error:any) { if(error.code==='ENOENT') return removed; throw error; }
      for (const hash of names) {
        if (!/^[a-f0-9]{64}$/.test(hash)) continue;
        const durable = await this.durable(hash);
        if (!durable || createHash('sha256').update(durable).digest('hex') !== hash) continue;
        const info=await lstat(this.file(hash)).catch((error:any)=>{if(error.code==='ENOENT')return undefined;throw error;});
        if(!info?.isFile()||info.isSymbolicLink()||info.size>this.maxBytes||info.size!==durable.length)continue;
        const pending = await readFile(this.file(hash)).catch((error:any) => { if(error.code==='ENOENT') return undefined; throw error; });
        if (!pending || !pending.equals(durable)) continue;
        await rm(this.file(hash)); removed.push(hash);
      }
      return removed;
    });
    this.queue = operation.catch(() => {});
    return operation;
  }
  async put(hash: string, bytes: Buffer) {
    return this.putBatch([{hash,bytes}]);
  }
  async putBatch(files: Array<{hash:string;bytes:Buffer}>) {
    if (!files.length || files.length > 64) throw new Error('Invalid upload cache batch size.');
    const unique = new Map<string,Buffer>();
    for (const {hash,bytes} of files) {
      this.file(hash);
      if (createHash('sha256').update(bytes).digest('hex') !== hash) throw new Error('Upload content hash mismatch.');
      unique.set(hash,bytes);
    }
    const operation = this.queue.then(async () => {
      await mkdir(this.root, { recursive: true });
      const missing = new Map<string,Buffer>();
      for (const [hash,bytes] of unique) if (!await this.get(hash)) missing.set(hash,bytes);
      if (!missing.size) return;
      let total = 0;
      for (const name of await readdir(this.root)) {
        const file = path.join(this.root, name);
        const info = await stat(file);
        if (Date.now() - info.mtimeMs > 24 * 60 * 60 * 1000) await rm(file, { force: true });
        else total += info.size;
      }
      if (total + [...missing.values()].reduce((sum,bytes)=>sum+bytes.length,0) > this.maxBytes) throw new Error('Pending upload cache is full. Retry after its 24-hour expiry.');
      for (const [hash,bytes] of missing) {
        const temporary = path.join(this.root, `${hash}.${randomUUID()}.tmp`);
        try { await writeFile(temporary, bytes); await rename(temporary, this.file(hash)); }
        finally { await rm(temporary, { force: true }); }
      }
    });
    this.queue = operation.catch(() => {});
    await operation;
  }
}
