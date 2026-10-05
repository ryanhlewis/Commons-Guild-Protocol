import { createServer, type Server } from 'node:http';
import { once } from 'node:events';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { afterEach, expect, it } from 'vitest';
import { serveStaticShardFile } from './static_shard_range.js';

const payload = Buffer.from('0123456789abcdef');
const servers: Server[] = [], directories: string[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
  await Promise.all(directories.splice(0).map(directory => fs.rm(directory, { recursive: true, force: true })));
});
async function fixture() {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'cgp-static-range-'));
  directories.push(directory);
  const file = path.join(directory, 'collision.vrwb'), empty = path.join(directory, 'empty.vrwb');
  await fs.writeFile(file, payload); await fs.writeFile(empty, Buffer.alloc(0));
  const server = createServer((req, res) => {
    res.setHeader('Cross-Origin-Embedder-Policy', 'credentialless');
    res.setHeader('Access-Control-Expose-Headers', 'X-Existing');
    if (req.url === '/strong') res.setHeader('etag', '"source-digest"');
    if (req.url === '/weak') res.setHeader('etag', 'W/"source-digest"');
    serveStaticShardFile(req, res, req.url === '/empty' ? empty : file, req.url === '/empty' ? 0 : payload.length, 'application/octet-stream');
  });
  servers.push(server); server.listen(0, '127.0.0.1'); await once(server, 'listening');
  return 'http://127.0.0.1:' + (server.address() as {port: number}).port;
}
it('serves closed, open, suffix and clamped intervals as exact byte slices over real HTTP', async () => {
  const base = await fixture();
  for (const [range, start, end] of [['bytes=2-5', 2, 5], ['bytes=12-', 12, 15], ['bytes=-4', 12, 15], ['bytes=12-999999999999999999999999999', 12, 15], ['BYTES=-99999999999999999999999999', 0, 15]] as const) {
    const response = await fetch(base, { headers: { range } });
    expect(response.status).toBe(206);
    expect(response.headers.get('content-range')).toBe(`bytes ${start}-${end}/${payload.length}`);
    expect(response.headers.get('content-length')).toBe(String(end - start + 1));
    expect(Buffer.from(await response.arrayBuffer())).toEqual(payload.subarray(start, end + 1));
    expect(response.headers.get('accept-ranges')).toBe('bytes');
    expect(response.headers.get('cross-origin-embedder-policy')).toBe('credentialless');
    expect(response.headers.get('access-control-expose-headers')).toContain('X-Existing');
    expect(response.headers.get('access-control-expose-headers')).toContain('Content-Range');
  }
});
it('returns 416 for valid unsatisfiable offsets, zero suffix and empty files without leaking body bytes', async () => {
  const base = await fixture();
  for (const [url, range, size] of [[base, 'bytes=16-', 16], [base, 'bytes=999999999999999999999999999-', 16], [base, 'bytes=-0', 16], [base + '/empty', 'bytes=0-0', 0], [base + '/empty', 'bytes=-1', 0]] as const) {
    const response = await fetch(url, { headers: { range } });
    expect(response.status).toBe(416); expect(response.headers.get('content-range')).toBe(`bytes */${size}`);
    expect(response.headers.get('content-length')).toBe('0'); expect((await response.arrayBuffer()).byteLength).toBe(0);
  }
});
it('ignores unsupported, malformed, multipart and HEAD ranges while retaining the full representation', async () => {
  const base = await fixture();
  for (const range of ['items=0-1', 'bytes=3-2', 'bytes=-', 'bytes=0-1,4-5', 'bytes=1e2-', 'bytes= 0-1']) {
    const response = await fetch(base, { headers: { range } });
    expect(response.status).toBe(200); expect(response.headers.get('content-range')).toBeNull();
    expect(Buffer.from(await response.arrayBuffer())).toEqual(payload);
  }
  const head = await fetch(base, { method: 'HEAD', headers: { range: 'bytes=0-1' } });
  expect(head.status).toBe(200); expect(head.headers.get('content-length')).toBe(String(payload.length));
  expect((await head.arrayBuffer()).byteLength).toBe(0);
});
it('uses existing strong If-Range validators and falls back to full bytes for weak, absent or mismatched validators', async () => {
  const base = await fixture();
  for (const [route, validator, status] of [['/strong', '"source-digest"', 206], ['/strong', '"old-digest"', 200], ['/weak', 'W/"source-digest"', 200], ['/', '"source-digest"', 200], ['/strong', 'Sun, 04 Oct 2026 00:00:00 GMT', 200]] as const) {
    const response = await fetch(base + route, { headers: { range: 'bytes=2-5', 'if-range': validator } });
    expect(response.status).toBe(status);
    expect(Buffer.from(await response.arrayBuffer())).toEqual(status === 206 ? payload.subarray(2, 6) : payload);
  }
});
