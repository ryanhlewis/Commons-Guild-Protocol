import { createReadStream } from 'node:fs';
import type { IncomingMessage, ServerResponse } from 'node:http';

type StaticByteRange = { start: number; end: number } | { unsatisfiable: true };

/** Unsupported/malformed/multipart fields are ignored; valid empty ranges are 416. */
export function staticSingleByteRange(value: unknown, size: number): StaticByteRange | undefined {
  if (typeof value !== 'string' || !Number.isSafeInteger(size) || size < 0) return undefined;
  const match = /^bytes=(\d*)-(\d*)$/i.exec(value.trim());
  if (!match || (!match[1] && !match[2])) return undefined;
  const first = match[1] ? Number(match[1]) : undefined;
  const last = match[2] ? Number(match[2]) : undefined;
  if (first !== undefined && last !== undefined && last < first) return undefined;
  if (!size || (first !== undefined && (!Number.isSafeInteger(first) || first >= size)) || (first === undefined && last === 0)) return { unsatisfiable: true };
  if (first === undefined) return { start: Math.max(0, size - last!), end: size - 1 };
  return { start: first, end: last === undefined ? size - 1 : Math.min(last, size - 1) };
}

/** Called only after the caller has authorized and contained the immutable file path. */
export function serveStaticShardFile(req: IncomingMessage, res: ServerResponse, filePath: string, size: number, contentType: string) {
  res.setHeader('content-type', contentType);
  res.setHeader('cache-control', 'public, max-age=31536000, immutable');
  res.setHeader('accept-ranges', 'bytes');
  const exposed = new Set(String(res.getHeader('access-control-expose-headers') || '').split(',').map(value => value.trim()).filter(Boolean));
  for (const header of ['Content-Range', 'Accept-Ranges', 'Content-Length', 'ETag']) exposed.add(header);
  res.setHeader('access-control-expose-headers', [...exposed].join(', '));
  const etag = res.getHeader('etag');
  // No existing strong validator means If-Range cannot authorize a partial reply.
  // Weak tags, dates and mismatches conservatively retain the full representation.
  const ifRange = req.headers['if-range'];
  const matchesIfRange = ifRange === undefined || (typeof etag === 'string' && /^"[^"\r\n]*"$/.test(etag) && ifRange === etag);
  const range = req.method === 'GET' && matchesIfRange ? staticSingleByteRange(req.headers.range, size) : undefined;
  if (range && 'unsatisfiable' in range) {
    res.statusCode = 416;
    res.setHeader('content-range', `bytes */${size}`);
    res.setHeader('content-length', 0);
    res.end();
    return;
  }
  res.statusCode = range ? 206 : 200;
  res.setHeader('content-length', range ? range.end - range.start + 1 : size);
  if (range) res.setHeader('content-range', `bytes ${range.start}-${range.end}/${size}`);
  if (req.method === 'HEAD' || !size) {
    res.end();
    return;
  }
  const stream = createReadStream(filePath, range ? { start: range.start, end: range.end } : undefined);
  stream.once('error', error => res.destroy(error));
  res.once('close', () => stream.destroy());
  stream.pipe(res);
}
