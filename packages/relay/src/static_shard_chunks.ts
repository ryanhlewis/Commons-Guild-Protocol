import { createHash } from 'node:crypto';
import { mkdir, open, readFile, rm, stat } from 'node:fs/promises';
import path from 'node:path';

export async function reassembleStaticFiles(root: string, value: unknown, maxBytes: number, maxFiles: number) {
  if (value === undefined) return;
  if (!Array.isArray(value) || value.length > maxFiles) throw new Error('Invalid large-file manifest.');
  const safe = (name: unknown) => {
    if (typeof name !== 'string' || !name || name.includes('\\') || name.includes(':') || name.startsWith('/') || name.split('/').some(p => !p || p === '.' || p === '..')) throw new Error('Unsafe chunk path.');
    return path.join(root, name);
  };
  let total = 0;
  const destinations = new Set<string>();
  const usedParts = new Set<string>();
  for (const file of value) {
    if (!file || !Array.isArray(file.parts) || !file.parts.length || file.parts.length > maxFiles || !Number.isSafeInteger(file.bytes) || file.bytes <= 0 || !/^[a-f0-9]{64}$/.test(file.sha256)) throw new Error('Invalid large-file entry.');
    total += file.bytes;
    if (total > maxBytes) throw new Error('Large files exceed extraction quota.');
    const destination = safe(file.path);
    if (file.path.startsWith('__hollow_chunks/') || destinations.has(destination)) throw new Error('Duplicate large-file destination.');
    destinations.add(destination);
    await mkdir(path.dirname(destination), {recursive: true});
    const output = await open(destination, 'wx');
    const hash = createHash('sha256');
    let bytes = 0;
    try {
      for (const part of file.parts) {
        const source = safe(part);
        if (!part.startsWith('__hollow_chunks/') || usedParts.has(part)) throw new Error('Invalid or reused chunk.');
        usedParts.add(part);
        const info = await stat(source);
        if (info.size > 25 * 1024 ** 2 || bytes + info.size > file.bytes) throw new Error('Large-file size mismatch.');
        const buffer = await readFile(source);
        hash.update(buffer); bytes += buffer.length;
        await output.writeFile(buffer);
      }
      if (bytes !== file.bytes || hash.digest('hex') !== file.sha256) throw new Error('Large-file integrity mismatch.');
    } catch (error) { await output.close(); await rm(destination, {force: true}); throw error; }
    await output.close();
  }
  for (const part of usedParts) await rm(safe(part));
}
