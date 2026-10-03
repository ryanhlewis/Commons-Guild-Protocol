import { unzipSync } from 'fflate';

/** Read ZIP sizes without inflating payloads. Extraction enforces the same limits. */
export function staticShardZipReservation(
  buffer: Uint8Array,
  limits: { bytes: number; files: number },
  accepts: (name: string) => boolean,
) {
  let bytes = 0, files = 0;
  unzipSync(buffer, { filter(file) {
    if (file.name.endsWith('/') || !accepts(file.name)) return false;
    files++;
    if (!Number.isSafeInteger(file.originalSize) || file.originalSize < 0)
      throw new Error('Invalid ZIP extraction size.');
    bytes += file.originalSize;
    if (files > limits.files || bytes > limits.bytes)
      throw new Error('Release exceeds playable extraction limits.');
    return false;
  } });
  return { bytes, files };
}

/** Reassembly temporarily keeps both the ZIP parts and the completed file. */
export function staticShardChunkReservation(value: unknown, maxBytes: number, maxFiles: number) {
  if (value === undefined) return 0;
  if (!Array.isArray(value) || value.length > maxFiles)
    throw new Error('Invalid large-file manifest.');
  let bytes = 0;
  for (const file of value) {
    if (!file || !Number.isSafeInteger(file.bytes) || file.bytes <= 0 ||
        !Array.isArray(file.parts) || !file.parts.length || file.parts.length > maxFiles)
      throw new Error('Invalid large-file entry.');
    bytes += file.bytes;
    if (bytes > maxBytes) throw new Error('Large files exceed extraction quota.');
  }
  return bytes;
}
