import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, readdir, rename, rm, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';

/** Disposable cache: the signed release remains the authority for every reference. */
export class StaticShardUploadCache {
  private queue: Promise<unknown> = Promise.resolve();
  constructor(private root: string, private maxBytes = 2 * 1024 ** 3) {}
  private file(hash: string) {
    if (!/^[a-f0-9]{64}$/.test(hash)) throw new Error('Invalid upload content hash.');
    return path.join(this.root, hash);
  }
  async get(hash: string): Promise<Buffer | undefined> {
    try {
      const bytes = await readFile(this.file(hash));
      if (createHash('sha256').update(bytes).digest('hex') !== hash) return undefined;
      return bytes;
    } catch (error: any) { if (error.code === 'ENOENT') return undefined; throw error; }
  }
  async put(hash: string, bytes: Buffer) {
    if (createHash('sha256').update(bytes).digest('hex') !== hash) throw new Error('Upload content hash mismatch.');
    const operation = this.queue.then(async () => {
      await mkdir(this.root, { recursive: true });
      if (await this.get(hash)) return;
      let total = 0;
      for (const name of await readdir(this.root)) {
        const file = path.join(this.root, name);
        const info = await stat(file);
        if (Date.now() - info.mtimeMs > 24 * 60 * 60 * 1000) await rm(file, { force: true });
        else total += info.size;
      }
      if (total + bytes.length > this.maxBytes) throw new Error('Pending upload cache is full. Retry after its 24-hour expiry.');
      const temporary = path.join(this.root, `${hash}.${randomUUID()}.tmp`);
      try { await writeFile(temporary, bytes); await rename(temporary, this.file(hash)); }
      finally { await rm(temporary, { force: true }); }
    });
    this.queue = operation.catch(() => {});
    await operation;
  }
}
